package Dict::Databases;
use strict;
use warnings;
use Dongry::Type::JSON;
use Dongry::Type::DateTime;

my $return_now = sub {
  require DateTime;
  return DateTime->now (time_zone => 'UTC');
}; # $return_now

my $onconnect = sub {
  my ($self, %args) = @_;
  $self->execute ('set time_zone = "+00:00"', [],
                  source_name => $args{source_name},
                  even_if_read_only => 1);
}; # $onconnect

$Dongry::Database::Registry->{dict} = {
  schema => {
    category => {
      type => {
        name_ja => 'text',
        name_en => 'text',
        name_fr => 'text',
        created_on => 'timestamp_as_DateTime',
        updated_on => 'timestamp_as_DateTime',
      },
      default => {
        created_on => $return_now,
        updated_on => $return_now,
      },
      primary_keys => [qw(category_id)],
    }, # category
    entry => {
      type => {
        title_ja => 'text',
        title_en => 'text',
        title_fr => 'text',
        created_on => 'timestamp_as_DateTime',
        updated_on => 'timestamp_as_DateTime',
      },
      default => {
        created_on => $return_now,
        updated_on => $return_now,
      },
      primary_keys => [qw(category_id entry_id)],
    }, # entry
    description => {
      type => {
        text => 'text_as_ref',
        metadata => 'json',
        created_on => 'timestamp_as_DateTime',
        updated_on => 'timestamp_as_DateTime',
      },
      default => {
        created_on => $return_now,
        updated_on => $return_now,
      },
      primary_keys => [qw(category_id entry_id lang)],
    },
  }, # schema
  onconnect => $onconnect,
}; # dict

$Dongry::Database::Registry->{dict}->{sources}->{default} = {
  dsn => $ENV{DSN_DICT},
};
$Dongry::Database::Registry->{dict}->{sources}->{master} = {
  dsn => $ENV{DSN_DICT},
  writable => 1,
};

1;
