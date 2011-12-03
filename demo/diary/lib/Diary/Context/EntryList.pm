package Diary::Context::EntryList;
use strict;
use warnings;

sub new {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub author_name {
  if (@_ > 1) {
    $_[0]->{author_name} = $_[1];
  }
  return $_[0]->{author_name};
} # author_name

sub author {
  if (@_ > 1) {
    $_[0]->{author} = $_[1];
  }
  return $_[0]->{author} //= do {
    require Diary::User;
    Diary::User->find_by_name ($_[0]->author_name); # or undef
  };
} # author

sub query {
  my $self = shift;
  require Diary::Query::Entry;
  return $self->{query} ||= Diary::Query::Entry->new
      (where => {author_id => $self->author->user_id},
       order => [qw(created_on DESC)]);
} # query

sub page {
  if (@_ > 1) {
    $_[0]->{page} = $_[1];
  }
  return $_[0]->{page} || 1;
} # page

sub per_page {
  if (@_ > 1) {
    $_[0]->{per_page} = $_[1];
  }
  return $_[0]->{per_page} || 5;
} # per_page

sub items {
  my $self = shift;
  return $self->{items} ||= $self->query->search
      (offset => ($self->page - 1) * $self->per_page,
       limit => $self->per_page);
} # items

1;
