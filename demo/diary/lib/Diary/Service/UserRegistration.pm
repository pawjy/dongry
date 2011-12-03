package Diary::Service::UserRegistration;
use strict;
use warnings;

sub new {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub create_user {
  my ($self, %args) = @_;

  require Dongry::Database;
  my $userdb = Dongry::Database->load ('user');
  my $user = $userdb->table ('user')->find ({name => $args{user_name}})
      || $userdb->table ('user')->find ({name => $args{user_name}},
                                        source_name => 'master')
      || $userdb->table ('user')->create ({name => $args{user_name}},
                                          flags => {is_new => 1});

  if ($user->flags->{is_new}) {
    $self->{user_created} = 1;
    $self->{user_name} = $args{user_name};
  } else {
    $self->{user_as_row} = $user;
  }
} # create_user

sub user_created {
  return $_[0]->{user_created};
}  # user_created

sub user {
  my $self = shift;
  return $self->{user} ||= do {
    require Diary::User;
    if ($self->{user_as_row}) {
      Diary::User->new_from_row ($self->{user_as_row});
    } elsif ($self->{user_name}) {
      Diary::User->find_by_name ($self->{user_name}, use_master => 1);
    } else {
      undef;
    }
  }
} # user

1;
