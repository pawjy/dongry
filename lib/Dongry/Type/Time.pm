package Dongry::Type::Time;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use Time::Local qw(timegm_nocheck);

$Dongry::Types->{timestamp} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00 00:00:00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/) {
      return timegm_nocheck $6, $5, $4, $3, $2-1, $1;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (defined $_[0]) {
      my @time = gmtime $_[0];
      return sprintf '%04d-%02d-%02d %02d:%02d:%02d',
          $time[5] + 1900, $time[4] + 1, $time[3],
          $time[2], $time[1], $time[0];
    } else {
      return '0000-00-00 00:00:00';
    }
  }, # serialize
}; # timestamp

$Dongry::Types->{timestamp_jst} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00 00:00:00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/) {
      return timegm_nocheck ($6, $5, $4, $3, $2-1, $1) - 9*60*60;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (defined $_[0]) {
      my @time = gmtime (9*60*60 + $_[0]);
      return sprintf '%04d-%02d-%02d %02d:%02d:%02d',
          $time[5] + 1900, $time[4] + 1, $time[3],
          $time[2], $time[1], $time[0];
    } else {
      return '0000-00-00 00:00:00';
    }
  }, # serialize
}; # timestamp_jst

$Dongry::Types->{date} = {
  parse => sub {
    if (not defined $_[0] or $_[0] eq '0000-00-00') {
      return undef;
    } elsif ($_[0] =~ /^([0-9]{4})-([0-9]{2})-([0-9]{2})$/) {
      return timegm_nocheck 0, 0, 0, $3, $2-1, $1;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    if (defined $_[0]) {
      my @time = gmtime $_[0];
      return sprintf '%04d-%02d-%02d',
          $time[5] + 1900, $time[4] + 1, $time[3];
    } else {
      return '0000-00-00';
    }
  }, # serialize
}; # date

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
