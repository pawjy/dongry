package Dongry::Type;
use strict;
use warnings;
our $VERSION = '1.0';

$Dongry::Types ||= {};

$Dongry::Types->{as_ref} = {
  parse => sub {
    return defined $_[0] ? \($_[0]) : undef;
  },
  serialize => sub {
    return defined $_[0] ? ${$_[0]} : undef;
  },
}; # as_ref

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
