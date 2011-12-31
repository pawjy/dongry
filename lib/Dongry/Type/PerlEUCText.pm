package Dongry::Type::PerlEUCText;
use strict;
use warnings;
our $VERSION = '1.0';
use Dongry::Type -Base;
use Encode;

$Dongry::Types->{perl_euc_text} = {
  parse => sub {
    return defined $_[0] ? decode 'euc-jp', $_[0] : $_[0];
  }, # parse
  serialize => sub {
    return defined $_[0] ? encode 'euc-jp', $_[0] : $_[0];
  }, # serialize
}; # perl_euc_text

$Dongry::Types->{perl_euc_text_as_ref} = {
  parse => sub {
    return defined $_[0] ? \decode 'euc-jp', $_[0] : $_[0];
  }, # parse
  serialize => sub {
    return defined $_[0] && defined ${$_[0]}
        ? encode 'euc-jp', ${$_[0]} : undef;
  }, # serialize
}; # perl_euc_text_as_ref

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
