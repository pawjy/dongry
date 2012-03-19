package Dongry::Type::MessagePack;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use Data::MessagePack;

$Dongry::Types->{messagepack} = {
  parse => sub {
    if (defined $_[0]) {
      return eval { Data::MessagePack->unpack ($_[0]) };
    } else {
      return undef;
    }
  },
  serialize => sub {
    if (defined $_[0]) {
      return Data::MessagePack->pack ($_[0]);
    } else {
      return undef;
    }
  },
}; # messagepack

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
