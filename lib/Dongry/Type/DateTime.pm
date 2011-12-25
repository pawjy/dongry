package Dongry::Type::DateTime;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use DateTime;
use Carp;

$Dongry::Types->{timestamp_as_DateTime} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00 00:00:00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/) {
      my $dt = eval {
        DateTime->new (year => $1, month => $2, day => $3,
                       hour => $4, minute => $5, second => $6,
                       time_zone => 'UTC');
      } or do {
        carp sprintf "TIMESTAMP |%s| is invalid", $_[0];
      };
      return $dt || undef;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (my $dt = $_[0]) {
      if ($dt->time_zone->name ne 'UTC' and
          $dt->time_zone->name ne 'floating') {
        $dt = $dt->clone;
        $dt->set_time_zone ('UTC');
      }
      return sprintf '%04d-%02d-%02d %02d:%02d:%02d',
          $dt->year, $dt->month, $dt->day,
          $dt->hour, $dt->minute, $dt->second;
    } else {
      return '0000-00-00 00:00:00';
    }
  }, # serialize
}; # timestamp_as_DateTime

$Dongry::Types->{timestamp_jst_as_DateTime} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00 00:00:00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/) {
      my $dt = eval {
        DateTime->new (year => $1, month => $2, day => $3,
                       hour => $4, minute => $5, second => $6,
                       time_zone => 'Asia/Tokyo');
      } or do {
        carp sprintf "TIMESTAMP |%s| is invalid", $_[0];
      };
      $dt->set_time_zone ('UTC') if $dt;
      return $dt || undef;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (my $dt = $_[0]) {
      if ($dt->time_zone->name eq 'floating') {
        $dt = $dt->clone;
        $dt->set_time_zone ('UTC');
        $dt->set_time_zone ('Asia/Tokyo');
      } elsif ($dt->time_zone->name ne 'Asia/Tokyo') {
        $dt = $dt->clone;
        $dt->set_time_zone ('Asia/Tokyo');
      }
      return sprintf '%04d-%02d-%02d %02d:%02d:%02d',
          $dt->year, $dt->month, $dt->day,
          $dt->hour, $dt->minute, $dt->second;
    } else {
      return '0000-00-00 00:00:00';
    }
  }, # serialize
}; # timestamp_jst_as_DateTime

$Dongry::Types->{date_as_DateTime} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2})$/) {
      my $dt = eval {
        DateTime->new (year => $1, month => $2, day => $3,
                       time_zone => 'UTC');
      } or do {
        carp sprintf "DATE |%s| is invalid", $_[0];
      };
      return $dt || undef;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (my $dt = $_[0]) {
      if ($dt->time_zone->name ne 'UTC' and
          $dt->time_zone->name ne 'floating') {
        $dt = $dt->clone;
        $dt->set_time_zone ('UTC');
      }
      return sprintf '%04d-%02d-%02d',
          $dt->year, $dt->month, $dt->day;
    } else {
      return '0000-00-00';
    }
  }, # serialize
}; # date_as_DateTime

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
