use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/lib');
use StandaloneTests;
use Dongry::SQL ();

for my $v (
  [undef, '``'],
  ['' => '``'],
  ['0' => '`0`'],
  ['abc' => '`abc`'],
  ['abv def' => '`abv def`'],
  ['a`bc\\' => '`a``bc\`'],
  ['```' => '````````'],
  ["\x{5000}" => "`\x{5000}`"],
  [(encode_web_utf8 "\x{5000}") => (encode_web_utf8 "`\x{5000}`")],
) {
  test {
    my $c = shift;
    is Dongry::SQL::quote $v->[0] => $v->[1];
    done $c;
  } n => 1, name => ['quote', $v->[0]];
}

for my $v (
      [undef, undef],
      ['' => ''],
      ['0' => '0'],
      ['abc' => 'abc'],
      ['abv def' => 'abv def'],
      ['a`bc\\' => 'a`bc\\\\'],
      ['```' => '```'],
      ["\x{5000}" => "\x{5000}"],
  [(encode_web_utf8 "\x{5000}") => (encode_web_utf8 "\x{5000}")],
  ['\%_' => '\\\\\\%\\_'],
  ['aq%%_b%ca%AX\e' => 'aq\\%\\%\\_b\\%ca\\%AX\\\\e'],
) {
  test {
    my $c = shift;
    is Dongry::SQL::like $v->[0] => $v->[1];
    done $c;
  } n => 1, name => ['like', $v->[0]];
}

RUN;

=head1 LICENSE

Copyright 2011-2017 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
