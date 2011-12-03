package Diary::User;
use strict;
use warnings;

sub new_from_row {
  return bless {row => $_[1]}, $_[0];
} # new_from_row

sub find_by_id {
  my ($class, $id) = @_;

  require Dongry::Database;
  my $userdb = Dongry::Database->load ('user');
  my $row = $userdb->table ('user')->find ({id => $id}) or return undef;
  return $class->new_from_row ($row);
} # find_by_id

sub find_by_name {
  my ($class, $name, %args) = @_;

  require Dongry::Database;
  my $userdb = Dongry::Database->load ('user');
  my $row = $userdb->table ('user')
      ->find ({name => $name},
              source_name => $args{use_master} ? 'master' : 'default')
      or return undef;
  return $class->new_from_row ($row);
} # find_by_name

sub user_id {
  return $_[0]->{row}->get ('id');
} # user_id

sub name {
  return $_[0]->{row}->get ('name');
} # name

1;
