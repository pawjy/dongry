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
  'author-name=s' => \$args{author_name},
  'diary-dsn=s' => \$args{diary_dsn},
  'entry-body=s' => \$args{entry_body},
  'entry-id=s' => \$args{entry_id},
  'entry-title=s' => \$args{entry_title},
  'page=s' => \$args{page},
  'user-dsn=s' => \$args{user_dsn},
  'user-name=s' => \$args{user_name},
) or do {
  $command = 'help';
};
$args{$_} = decode 'utf-8', $args{$_} for qw(entry_title entry_body);
binmode STDOUT, ':encoding(utf-8)';

use DBIx::ShowSQL;
require Diary::Config::Databases;
Diary::Config::Databases->setup
    (user_dsn => $args{user_dsn},
     diary_dsn => $args{diary_dsn});

my $CommandHandlers = {
  addentry => \&add_entry,
  adduser => \&add_user,
  help => \&help,
  showentry => \&show_entry,
  showentrylist => \&show_entry_list,
};

my $handler = $CommandHandlers->{$command} || $CommandHandlers->{help};
$handler->(%args);

sub add_user (%) {
  my %args = @_;
  
  require Diary::Service::UserRegistration;
  my $service = Diary::Service::UserRegistration->new;
  $service->create_user
      (user_name => $args{user_name});
  if ($service->user_created) {
    my $user = $service->user;
    printf "New user |%s| (id = %s) created\n",
        $user->name, $user->user_id;
  } else {
    my $user = $service->user;
    printf "Error: There is already user |%s| (id = %s)\n",
        $user->name, $user->user_id;
  }
} # add_user

sub add_entry (%) {
  my %args = @_;

  require Diary::User;
  my $user = Diary::User->find_by_name ($args{user_name})
      or die "User |$args{user_name}| not found";

  require Diary::Service::EditEntry;
  my $service = Diary::Service::EditEntry->new (user => $user);
  $service->add_entry
      (title => $args{entry_title}, body_as_ref => \$args{entry_body});

  if ($service->entry) {
    print "Entry added: ", $service->entry->as_line, "\n";
  }
} # add_entry

sub show_entry (%) {
  my %args = @_;

  require Diary::Entry;
  my $entry = Diary::Entry->find_by_id ($args{entry_id})
      or die "Entry |$args{entry_id}| not found";
  my $author = $entry->author;
  
  print $entry->as_line, "\n";
  printf "Author: %s (%d)\n", $author->name, $author->user_id;
  printf "Date: %s %s\n", $entry->created_on->ymd ('/'), $entry->created_on->hms (':');
  printf "Title: %s\n", $entry->title;
  printf "Body: %s\n", ${$entry->body_as_ref};
} # show_entry

sub show_entry_list (%) {
  my %args = @_;

  require Diary::Context::EntryList;
  my $app = Diary::Context::EntryList->new
      (author_name => $args{author_name},
       page => $args{page});
  die "Author |$args{author_name}| not found" unless $app->author;

  printf "%s's entries (%d of %d)\n",
      $app->author->name,
      $app->per_page,
      $app->query->count;
  $app->items->each (sub {
    my $entry = $_;
    print $entry->as_line, "\n";
  });
} # show_entry_list

sub help (%) {
  print "Available commands:\n";
  print "$0 help\n";
  print "$0 adduser --user-dsn=dsn --user-name=username\n";
  print "$0 addentry --user-dsn=dsn --user-name=username --diary-dsn=dsn --entry-title=title --entry-body=body\n";
  print "$0 showentry --user-dsn=dsn --diary-dsn=dsn --entry-id=eid";
} # help
