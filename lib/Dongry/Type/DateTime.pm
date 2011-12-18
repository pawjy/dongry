package Dongry::Type::DateTime;
use strict;
use warnings;
use DateTime::Format::MySQL;
use Carp;

# XXX
push @Dongry::Table::CARP_NOT, __PACKAGE__;
push @Dongry::Table::Row::CARP_NOT, __PACKAGE__;

$Dongry::Types->{timestamp_as_DateTime} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00 00:00:00') {
      return undef;
    } else {
      my $dt = eval { DateTime::Format::MySQL->parse_datetime ($_[0]) } or do {
        croak $@; # XXX
      };
      $dt->set_time_zone ('UTC');
      return $dt;
    }
  }, # parse
  serialize => sub {
    if ($_[0]) {
      if ($_[0]->time_zone->name ne 'UTC' and
          $_[0]->time_zone->name ne 'floating') {
        my $dt = $_[0]->clone;
        $dt->set_time_zone ('UTC');
        return DateTime::Format::MySQL->format_datetime ($dt);
      } else {
        return DateTime::Format::MySQL->format_datetime ($_[0]);
      }
    } else {
      return '0000-00-00 00:00:00';
    }
  }, # serialize
}; # timestamp_as_DateTime

1;
