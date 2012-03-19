package test::Dongry::SQL::fields;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL;

sub bare_sql_fragment ($) {
  return bless \('' . $_[0]), 'Dongry::SQL::BareFragment';
} # bare_sql_fragment

sub _fields_valid : Test(42) {
  for (
    [undef, '*'],
    ['abc', '`abc`'],
    ['ab `\\c', '`ab ``\\c`'],
    ['abc', '`abc`'],
    ["\x{4010}\x{124}ab", qq{`\x{4010}\x{124}ab`}],
    [['a', 'bcd'], '`a`, `bcd`'],
    [['a', "\x{4010}\x{124}ab"], qq{`a`, `\x{4010}\x{124}ab`}],
    [['a', ['b', ['c']]] => '`a`, `b`, `c`'],
    [bare_sql_fragment ('ab cde') => 'ab cde'],
    [bare_sql_fragment ("ab\x{8000} cde") => "ab\x{8000} cde"],
    [['a', bare_sql_fragment ('count(b) as c')] => '`a`, count(b) as c'],
    [{-count => undef} => 'COUNT(*)'],
    [{-count => 1} => 'COUNT(`1`)'],
    [{-count => 'a'} => 'COUNT(`a`)'],
    [{-count => "\x{5000}"} => qq{COUNT(`\x{5000}`)}],
    [{-count => ['a', 'b']} => 'COUNT(`a`, `b`)'],
    [{-count => 'a', as => 'ab c'} => 'COUNT(`a`) AS `ab c`'],
    [{-count => 'a', as => "\x{8000}"} => qq{COUNT(`a`) AS `\x{8000}`}],
    [{-count => undef, distinct => 1} => 'COUNT(DISTINCT *)'],
    [{-count => 'a', distinct => 1} => 'COUNT(DISTINCT `a`)'],
    [{-count => "\x{5000}", distinct => 1} => qq{COUNT(DISTINCT `\x{5000}`)}],
    [{-count => ['a', 'b'], distinct => 1} => 'COUNT(DISTINCT `a`, `b`)'],
    [{-count => 'a', distinct => 1, as => 'ab c'}
         => 'COUNT(DISTINCT `a`) AS `ab c`'],
    [{-count => 'a', distinct => 1, as => "\x{8000}"}
         => qq{COUNT(DISTINCT `a`) AS `\x{8000}`}],
    [{-min => 'a', distinct => 1, as => "\x{8000}"}
         => qq{MIN(DISTINCT `a`) AS `\x{8000}`}],
    [{-max => 'a', distinct => 1, as => "\x{8000}"}
         => qq{MAX(DISTINCT `a`) AS `\x{8000}`}],
    [{-sum => 'a', distinct => 1, as => "\x{8000}"}
         => qq{SUM(DISTINCT `a`) AS `\x{8000}`}],
    ['' => '``'],
    [['a', undef] => '`a`, *'],
    [{-count => [{-sum => 'hoge', as => 'abc'}]} => 'COUNT(SUM(`hoge`))'],
    [{-column => 'ho`ge'} => '`ho``ge`'],
    [{-column => 'ho`ge', as => '`'} => '`ho``ge` AS ````'],
    [{-max => {-column => 'ho`ge', as => '`'}} => 'MAX(`ho``ge`)'],
    [{-date => 'created', as => 'date'} => 'DATE(`created`) AS `date`'],
    [{-max => {-date => 'created'}, as => 'date'}
         => 'MAX(DATE(`created`)) AS `date`'],
    [{-count => {-date => 'created'}, as => 'date', distinct => 1}
         => 'COUNT(DISTINCT DATE(`created`)) AS `date`'],
    [{-date => 'created', as => 'date', delta => 0}
         => 'DATE(`created`) AS `date`'],
    [{-date => 'created', as => 'date', delta => 120}
         => 'DATE(`created` + INTERVAL 120 SECOND) AS `date`'],
    [{-date => 'created', as => 'date', delta => -120}
         => 'DATE(`created` + INTERVAL -120 SECOND) AS `date`'],
    [{-date => 'created', as => 'date', delta => 'abc'}
         => 'DATE(`created` + INTERVAL 0 SECOND) AS `date`'],
    [{-distance => 'lat`lon', lat => -10.211111, lon => 20.24222111,
      as => 'ho`ge'}
         => qq<GLength(GeomFromText(CONCAT('LineString(20.2422211100 -10.2111110000,', X(`lat``lon`), ' ', Y(`lat``lon`),')'))) AS `ho``ge`>],
  ) {
    eq_or_diff fields $_->[0], $_->[1];
  }
} # _fields_valid

sub _fields_invalid : Test(5) {
  for (
    [],
    {},
    {-hoge => 1},
    {count => 1},
    [bless {}, 'hoge'],
  ) {
    dies_here_ok {
      fields $_;
    };
  }
} # _fields_invalid

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
