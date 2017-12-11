package StandaloneTests;
use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child
    ('t_deps/modules/*/lib');
use Carp;

our @EXPORT;

sub import ($;@) {
  my $from_class = shift;
  my ($to_class, $file, $line) = caller;
  no strict 'refs';
  for (@_ ? @_ : @{$from_class . '::EXPORT'}) {
    my $code = $from_class->can ($_)
        or croak qq{"$_" is not exported by the $from_class module at $file line $line};
    *{$to_class . '::' . $_} = $code;
  }
} # import

use Test::More;
use Test::X1;
use Web::Encoding;
push @EXPORT, grep { not /^\$/ } @Test::More::EXPORT;
push @EXPORT, @Test::X1::EXPORT;
push @EXPORT, @Web::Encoding::EXPORT;

push @EXPORT, 'RUN';
*RUN = \&Test::X1::run_tests;

1;

=head1 LICENSE

Copyright 2017 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
