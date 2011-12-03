package Diary::Entry;
use strict;
use warnings;

sub new_from_row {
  my $class = shift;
  return bless {row => $_[0]}, $class;
} # new_from_row

sub find_by_id {
  my ($class, $id) = @_;

  require Dongry::Database;
  my $diarydb = Dongry::Database->load ('diary');
  my $row = $diarydb->table ('entry')->find ({id => $id}) or return undef;
  return $class->new_from_row ($row);
} # find_by_id

sub entry_id {
  return $_[0]->{row}->get ('id');
} # entry_id

sub author_id {
  return $_[0]->{row}->get ('author_id');
} # author_id

sub author {
  if (@_ > 1) {
    $_[0]->{author} = $_[1];
  }
  return $_[0]->{author} ||= do {
    require Diary::User;
    Diary::User->find_by_id ($_[0]->author_id);
  };
} # author

sub set_author_as_row {
  require Diary::User;
  $_[0]->{author} = Diary::User->new_from_row ($_[1]);
} # set_author_as_row

sub title {
  return $_[0]->{row}->get ('title');
} # title

sub body_as_ref {
  return $_[0]->{row}->get ('body');
} # body_as_ref

sub body_as_summary {
  my $self = shift;
  my $bodyref = $self->body_as_ref;
  if (length $$bodyref > 10) {
    return substr ($$bodyref, 0, 7) . '...';
  } else {
    return $$bodyref;
  }
} # body_as_summary

sub created_on {
  return $_[0]->{row}->get ('created_on');
} # created_on

sub as_line {
  my $self = shift;
  my $row = $self->{row};
  return sprintf "#%d: %s at %s %s by %s : %s",
      $self->entry_id,
      $self->title,
      $self->created_on->ymd ('/'),
      $self->created_on->hms (':'),
      $self->author->name,
      $self->body_as_summary;
} # as_line

1;
