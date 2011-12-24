package Dongry::Type;
use strict;
use warnings;
our $VERSION = '1.0';
use Encode;

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

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
