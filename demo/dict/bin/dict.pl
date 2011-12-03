#!/usr/bin/perl
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use lib file (__FILE__)->dir->parent->parent->parent->subdir ('lib')->stringify;
use lib glob file (__FILE__)->dir->parent->parent->parent->subdir ('modules', '*', 'lib')->stringify;
use Getopt::Long;
use Encode;

my $command = shift || 'help';

my %args;
GetOptions (
  'category-name=s' => \$args{category_name},
  'entry-body=s' => \$args{entry_body},
  'entry-keyword=s' => sub {
    push @{$args{entry_keywords} ||= []}, $_[1];
  },
  'entry-title=s' => \$args{entry_title},
  'lang=s' => \$args{lang},
  'page=s' => \$args{page},
) or do {
  $command = 'help';
};
$args{$_} = decode 'utf-8', $args{$_} for qw(entry_title entry_body);
binmode STDOUT, ':encoding(utf-8)';

use DBIx::ShowSQL;

my $CommandHandlers = {
  categorylist => \&category_list,
  editentry => \&edit_entry,
  entrylist => \&entry_list,
  help => \&help,
};

my $handler = $CommandHandlers->{$command} || $CommandHandlers->{help};
$handler->(%args);

sub category_list (%) {
  my %args = @_;
  my $lang = $args{lang};
  
  require Dict::Context::CategoryList;
  my $app = Dict::Context::CategoryList->new
      (page => $args{page});
  $app->items->each (sub {
    printf "#%d %s (Updated: %s %s)\n",
        $_->category_id, $_->name_in_lang ($lang),
        $_->updated_on->ymd ('-'), $_->updated_on->hms (':');
  });
} # category_list

sub entry_list (%) {
  my %args = @_;
  my $lang = $args{lang};

  require Dict::Context::EntryList;
  my $app = Dict::Context::EntryList->new
      (category_name => $args{category_name},
       category_name_lang => $lang,
       page => $args{page});
  $app->items->each (sub {
    printf "#%d %s (Updated: %s %s)\n",
        $_->entry_id, $_->title_in_lang ($lang),
        $_->updated_on->ymd ('-'), $_->updated_on->hms (':');
    print "  ";
    print $_->keywords_in_lang ($lang)->join (', ');
    print "\n";
    printf "  %s\n",
        ${$_->description_in_lang_as_ref ($lang) or \''};
  });
} # entry_list

sub edit_entry (%) {
  my %args = @_;

  require Dict::Service::EditEntry;
  my $service = Dict::Service::EditEntry->new (lang => $args{lang});

  $service->create_category_if_necessary
      (category_name => $args{category_name});

  $service->create_entry_if_necessary
      (entry_title => $args{entry_title});

  $service->update_text
      (entry_body_as_ref => \$args{entry_body},
       keywords => $args{entry_keywords});

  warn "Category ID: " . $service->category->category_id . "\n";
  warn "Entry ID: " . $service->entry->entry_id . "\n";
  warn ${$service->entry->description_in_lang_as_ref ($args{lang})}, "\n";
} # edit_entry

sub help (%) {
  print "Available commands:\n";
  print "$0 help\n";
  print "DSN_DICT=dsn $0 categorylist --lang=lang --page=n\n";
  print "DSN_DICT=dsn $0 entrylist --lang=lang --category-name=string --page=n\n";
  print "DSN_DICT=dsn $0 editentry --lang=lang --category-name=string --entry-title=string --entry-keyword=string --entry-body=string\n";
} # help
