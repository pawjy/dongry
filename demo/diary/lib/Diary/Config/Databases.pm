package Diary::Config::Databases;
use strict;
use warnings;
use Dongry::Type::DateTime;

my $onconnect = sub {
  my ($self, %args) = @_;
  $self->execute ('set time_zone = "+00:00"', [],
                  source_name => $args{source_name},
                  even_if_read_only => 1);
}; # $onconnect

sub setup ($%) {
  my (undef, %args) = @_;
  
  $Dongry::Database::Registry->{user} = {
    sources => {
      default => {
        dsn => $args{user_dsn},
      }, # default
      master => {
        dsn => $args{user_dsn},
        writable => 1,
      }, # master
    }, # sources
    schema => {
      user => {
        type => {
          name => 'text',
        },
      }, # user
    },
    onconnect => $onconnect,
  }; # user
  
  $Dongry::Database::Registry->{diary} = {
    sources => {
      default => {
        dsn => $args{diary_dsn},
      }, # default
      master => {
        dsn => $args{diary_dsn},
        writable => 1,
      }, # master
    }, # sources
    schema => {
      entry => {
        type => {
          title => 'text',
          body => 'text_as_ref',
          created_on => 'timestamp_as_DateTime',
        },
        default => {
          created_on => sub { require DateTime; return DateTime->now (time_zone => 'UTC') },
        },
      }, # user
    }, # entry
    onconnect => $onconnect,
  }; # diary
} # setup

1;
