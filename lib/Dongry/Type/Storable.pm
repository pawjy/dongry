package Dongry::Type::Storable;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use Storable qw(nfreeze thaw);

$Dongry::Types->{storable_nfreeze} = {
  parse => sub {
    if (defined $_[0]) {
      return eval { thaw $_[0] }; # or undef
    } else {
      return undef;
    }
  },
  serialize => sub {
    if (defined $_[0]) {
      return eval { nfreeze $_[0] }; # or undef
    } else {
      return undef;
    }
  },
}; # storable_nfreeze

1;

=head1 LICENSE

Copyright 2012 Hatena <http://www.hatena.ne.jp/>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
