package Diary::Service::EditEntry;
use strict;
use warnings;

sub new {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub user {
  if (@_ > 1) {
    $_[0]->{user} = $_[1];
  }
  return $_[0]->{user};
} # user

sub entry {
  if (@_ > 1) {
    $_[0]->{entry} = $_[1];
  }
  return $_[0]->{entry};
} # entry

sub add_entry {
  my ($self, %args) = @_;

  require Dongry::Database;
  my $diarydb = Dongry::Database->load ('diary');
  my $entry = $diarydb->table ('entry')->create
      ({id => int rand 100000,
        author_id => $self->user->user_id,
        title => $args{title},
        body => $args{body_as_ref}});

  require Diary::Entry;
  $self->{entry} = Diary::Entry->new_from_row ($entry);
}

1;
