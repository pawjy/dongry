package Dongry::Type;
use strict;
use warnings;
our $VERSION = '1.0';
use Encode;

sub import {
  my $opt = $_[1] || '';
  if ($opt eq -Base) {
    my $caller = caller;
    push @Dongry::Table::CARP_NOT, $caller;
    push @Dongry::Table::Row::CARP_NOT, $caller;
  }
} # import

$Dongry::Types ||= {};

$Dongry::Types->{as_ref} = {
  parse => sub {
    return defined $_[0] ? \($_[0]) : undef;
  },
  serialize => sub {
    return defined $_[0] ? ${$_[0]} : undef;
  },
}; # as_ref

$Dongry::Types->{text} = {
  parse => sub {
    return defined $_[0] ? decode 'utf-8', $_[0] : $_[0];
  }, # parse
  serialize => sub {
    return defined $_[0] ? encode 'utf-8', $_[0] : $_[0];
  }, # serialize
}; # text

$Dongry::Types->{text_as_ref} = {
  parse => sub {
    return defined $_[0] ? \decode 'utf-8', $_[0] : $_[0];
  }, # parse
  serialize => sub {
    return defined $_[0] && defined ${$_[0]}
        ? encode 'utf-8', ${$_[0]} : undef;
  }, # serialize
}; # text_as_ref

$Dongry::Types->{null_filled} = {
  parse => sub {
    if (defined $_[0]) {
      my $v = $_[0];
      $v =~ s/\x00+\z//;
      return $v;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    return defined $_[0] ? encode 'utf-8', $_[0] : $_[0];
  }, # serialize
}; # null_filled

$Dongry::Types->{text_null_filled} = {
  parse => sub {
    if (defined $_[0]) {
      my $v = $_[0];
      $v =~ s/\x00+\z//;
      return decode 'utf-8', $v;
    } else {
      return undef;
    }
  }, # parse
  serialize => sub {
    return defined $_[0] ? encode 'utf-8', $_[0] : $_[0];
  }, # serialize
}; # text_null_filled

$Dongry::Types->{set} = {
  parse => sub {
    return defined $_[0] ? {map { $_ => 1 } split /,/, $_[0]} : {};
  }, # parse
  serialize => sub {
    return join ',', grep { $_[0]->{$_} } keys %{$_[0] or {}};
  }, # serialize
}; # set

sub parse ($$$) {
  #my ($class, $type, $value) = @_;
  return $Dongry::Types->{$_[1]}->{parse}->($_[2])
} # parse

sub serialize ($$$) {
  #my ($class, $type, $value) = @_;
  return $Dongry::Types->{$_[1]}->{serialize}->($_[2])
} # serialize

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
