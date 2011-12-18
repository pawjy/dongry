package test::Dongry::SQL::quote;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL qw(quote);
use Encode;

sub _quote : Test(9) {
  for (
      [undef, '``'],
      ['' => '``'],
      ['0' => '`0`'],
      ['abc' => '`abc`'],
      ['abv def' => '`abv def`'],
      ['a`bc\\' => '`a``bc\`'],
      ['```' => '````````'],
      ["\x{5000}" => "`\x{5000}`"],
      [(encode 'utf-8', "\x{5000}") => (encode 'utf-8', "`\x{5000}`")],
  ) {
    is quote $_->[0] => $_->[1];
  }
} # _quote

sub _like : Test(11) {
  for (
      [undef, undef],
      ['' => ''],
      ['0' => '0'],
      ['abc' => 'abc'],
      ['abv def' => 'abv def'],
      ['a`bc\\' => 'a`bc\\\\'],
      ['```' => '```'],
      ["\x{5000}" => "\x{5000}"],
      [(encode 'utf-8', "\x{5000}") => (encode 'utf-8', "\x{5000}")],
      ['\%_' => '\\\\\\%\\_'],
      ['aq%%_b%ca%AX\e' => 'aq\\%\\%\\_b\\%ca\\%AX\\\\e'],
  ) {
    is Dongry::SQL::like $_->[0] => $_->[1];
  }
} # _like

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
