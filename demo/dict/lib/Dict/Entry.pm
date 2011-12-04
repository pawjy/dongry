package Dict::Entry;
use strict;
use warnings;
use Dict::Databases;

sub new_from_category_and_title_in_lang {
  my ($class, $cat, $title, $lang) = @_;
  return bless {category => $cat, title => $title, lang => $lang}, $class;
} # find_by_category_and_title

sub new_from_entry_row {
  my ($class, $row) = @_;
  return bless {entry_row => $row}, $class;
} # entry_row

sub entry_row {
  my $self = shift;
  return $self->{entry_row} if exists $self->{entry_row};
  if ($self->{category} and defined $self->{title} and defined $self->{lang}) {
    require Dongry::Database;
    return $self->{entry_row}
        = Dongry::Database->load ('dict')->table ('entry')
            ->find ({category_id => $self->{category}->category_id,
                     'title_' . $self->{lang} => $self->{title}});
  } else {
    return $self->{entry_row} = undef;
  }
} # entry_row

sub description_row_in_lang {
  my ($self, $lang) = @_;
  return $self->{description_rows}->{$lang}
      if $self->{description_rows};

  require Dongry::Database;
  Dongry::Database->load ('dict')
      ->select ('description',
                {category_id => $self->category->category_id,
                 entry_id => $self->entry_id})
      ->each_as_row (sub {
    $self->{description_rows}->{$_->get ('lang')} = $_;
  });

  return $self->{description_rows}->{$lang}; # or undef
} # description_row_in_lang

sub set_description_rows {
  my ($self, $rows) = @_;
  my $map = $self->{description_rows} = {};
  $rows->each (sub { $map->{$_->get ('lang')} = $_ });
} # set_description_rows

sub category {
  if ($_[0]->{category}) {
    return $_[0]->{category};
  } elsif ($_[0]->{entry_row}) {
    require Dict::Category;
    return Dict::Category->new_from_category_id
        ($_[0]->{entry_row}->get ('category_id'));
  } else {
    return undef;
  }
} # category

sub category_id {
  if ($_[0]->{category_id}) {
    return $_[0]->{category_id};
  } else {
    my $cat = $_[0]->category;
    if ($cat) {
      return $cat->category_id;
    } else {
      return undef;
    }
  }
} # category_id

sub entry_id {
  my $row = $_[0]->entry_row or return undef;
  return $row->get ('entry_id');
} # entry_id

sub title_in_lang {
  my ($self, $lang) = @_;
  my $row = $self->entry_row or return undef;
  return $row->get ('title_' . $lang);
} # title_in_lang

sub description_in_lang_as_ref {
  my ($self, $lang) = @_;
  my $row = $self->description_row_in_lang ($lang) or return undef;
  return $row->get ('text');
} # description_in_lang_as_ref

sub keywords_in_lang {
  my ($self, $lang) = @_;
  require List::Rubyish;
  my $row = $self->description_row_in_lang ($lang) or
      return List::Rubyish->new;
  return List::Rubyish->new ([@{($row->get ('metadata') || {})->{keywords} || []}]);
} # keywords_in_lang

sub updated_on {
  my $row = $_[0]->entry_row or return undef;
  return $row->get ('updated_on');
} # updated_on

1;
