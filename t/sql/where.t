package test::Dongry::SQL::where;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL;
use Encode;

$Dongry::SQL::SortKeys = 1;

sub _where_valid_hashref : Test(48) {
  for (
      [{foo => undef} => ['`foo` IS NULL', []]],
      [{foo => ''} => ['`foo` = ?', ['']]],
      [{foo => '0'} => ['`foo` = ?', ['0']]],
      [{foo => 'bar'} => ['`foo` = ?', ['bar']]],
      [{foo => {-eq => undef}} => ['`foo` IS NULL', []]],
      [{foo => {-eq => ''}} => ['`foo` = ?', ['']]],
      [{foo => {-eq => '0'}} => ['`foo` = ?', ['0']]],
      [{foo => {-eq => 'bar'}} => ['`foo` = ?', ['bar']]],
      [{foo => {'==' => undef}} => ['`foo` IS NULL', []]],
      [{foo => {'==' => 'bar'}} => ['`foo` = ?', ['bar']]],
      [{foo => {-ne => undef}} => ['`foo` IS NOT NULL', []]],
      [{foo => {-ne => ''}} => ['`foo` != ?', ['']]],
      [{foo => {-ne => '0'}} => ['`foo` != ?', ['0']]],
      [{foo => {-ne => 'bar'}} => ['`foo` != ?', ['bar']]],
      [{foo => {-not => undef}} => ['`foo` IS NOT NULL', []]],
      [{foo => {-not => 'bar'}} => ['`foo` != ?', ['bar']]],
      [{foo => {'!=' => undef}} => ['`foo` IS NOT NULL', []]],
      [{foo => {'!=' => 'bar'}} => ['`foo` != ?', ['bar']]],
      [{foo => {-lt => 'bar'}} => ['`foo` < ?', ['bar']]],
      [{foo => {'<' => 'bar'}} => ['`foo` < ?', ['bar']]],
      [{foo => {-le => 'bar'}} => ['`foo` <= ?', ['bar']]],
      [{foo => {'<=' => 'bar'}} => ['`foo` <= ?', ['bar']]],
      [{foo => {-gt => 'bar'}} => ['`foo` > ?', ['bar']]],
      [{foo => {'>' => 'bar'}} => ['`foo` > ?', ['bar']]],
      [{foo => {-ge => 'bar'}} => ['`foo` >= ?', ['bar']]],
      [{foo => {'>=' => 'bar'}} => ['`foo` >= ?', ['bar']]],
      [{foo => {-regexp => '^b.*\+ar?'}} => ['`foo` REGEXP ?', ['^b.*\+ar?']]],
      [{foo => {-like => 'b\%a_ar'}} => ['`foo` LIKE ?', ['b\%a_ar']]],
      [{foo => {-prefix => ''}} => ['`foo` LIKE ?', ['%']]],
      [{foo => {-prefix => '0'}} => ['`foo` LIKE ?', ['0%']]],
      [{foo => {-prefix => 'b\%a_a'}} => ['`foo` LIKE ?', ['b\\\\\\%a\\_a%']]],
      [{foo => {-suffix => ''}} => ['`foo` LIKE ?', ['%']]],
      [{foo => {-suffix => '0'}} => ['`foo` LIKE ?', ['%0']]],
      [{foo => {-suffix => 'b\%a_a'}} => ['`foo` LIKE ?', ['%b\\\\\\%a\\_a']]],
      [{foo => {-infix => ''}} => ['`foo` LIKE ?', ['%%']]],
      [{foo => {-infix => '0'}} => ['`foo` LIKE ?', ['%0%']]],
      [{foo => {-infix => 'b\%a_a'}} => ['`foo` LIKE ?', ['%b\\\\\\%a\\_a%']]],
      [{foo => {-in => ['']}} => ['`foo` IN (?)', ['']]],
      [{foo => {-in => ['0']}} => ['`foo` IN (?)', ['0']]],
      [{foo => {-in => ['a']}} => ['`foo` IN (?)', ['a']]],
      [{foo => {-in => [1, 'a']}} => ['`foo` IN (?, ?)', ['1', 'a']]],
      [{foo => {-in => [1, 'a', 33]}}
           => ['`foo` IN (?, ?, ?)', ['1', 'a', 33]]],
      [{foo => {-in => bless [1, 'a', 33], 'List::Rubyish'}}
           => ['`foo` IN (?, ?, ?)', ['1', 'a', 33]]],
      [{foo => {-in => bless [1, 'a', 33], 'DBIx::MoCo::List'}}
           => ['`foo` IN (?, ?, ?)', ['1', 'a', 33]]],
      [{foo => 'bar', 'baz' => 'hoge'}
           => ['`baz` = ? AND `foo` = ?', ['hoge', 'bar']]],
      [{foo => 'bar', 'baz' => undef}
           => ['`baz` IS NULL AND `foo` = ?', ['bar']]],
      [{foo => 'bar', 'baz' => {-not => undef}}
           => ['`baz` IS NOT NULL AND `foo` = ?', ['bar']]],
      [{foo => 'bar', 'baz' => {-not => undef}, 'ab `c)' => {-lt => 120}}
           => ['`ab ``c)` < ? AND `baz` IS NOT NULL AND `foo` = ?',
               [120, 'bar']]],
  ) {
    eq_or_diff [where ($_->[0])], $_->[1];
  }
} # _where_valid_hashref

sub _where_bad_operator : Test(6) {
  for (
      {foo => {}},
      {foo => {-hoge => 1243}},
      {foo => {eq => 1243}},
      {foo => {'-' => 1243}},
      {foo => {-0 => 1243}},
      {foo => {0 => 1243}},
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_bad_operator

sub _where_undef_value : Test(11) {
  for (
      {foo => {-le => undef}},
      {foo => {-ge => undef}},
      {foo => {-lt => undef}},
      {foo => {-gt => undef}},
      {foo => {-like => undef}},
      {foo => {-prefix => undef}},
      {foo => {-suffix => undef}},
      {foo => {-infix => undef}},
      {foo => {-in => undef}},
      {foo => {-in => [undef]}},
      {foo => {-in => [124, undef]}},
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_undef_value

sub _where_ref_value : Test(18) {
  for (
      {foo => \'abc'},
      {foo => ['abc']},
      {foo => bless ['abc'], 'test::hoge'},
      {foo => {-lt => \'abc'}},
      {foo => {-lt => ['abc']}},
      {foo => {-lt => {'abc' => 4}}},
      {foo => {-lt => bless {'abc' => 4}, 'test::hoge'}},
      {foo => {-regexp => \'abc'}},
      {foo => {-prefix => \'abc'}},
      {foo => {-prefix => ['abc']}},
      {foo => {-prefix => ['abc', {cd => 12}]}},
      {foo => {-in => [\'']}},
      {foo => {-in => [\'abc']}},
      {foo => {-in => [bless [], 'test::hoge']}},
      {foo => {-in => [{'abc' => 5}]}},
      {foo => {-in => [['abc']]}},
      {foo => {-in => [124, \'abc']}},
      {foo => {-in => [124, \'abc', bless {}, 'test::hofe']}},
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_ref_value

sub _where_empty_value : Test(3) {
  for (
      {foo => {-in => []}},
      {foo => {-in => bless [], 'List::Rubyish'}},
      {foo => {-in => bless [], 'DBIx::MoCo::List'}},
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_empty_value

sub _where_bad_value : Test(3) {
  for (
      {foo => {-in => 'abc'}},
      {foo => {-in => {}}},
      {foo => {-in => bless {}, 'test::hofe'}},
  ) {
    dies_ok {
      where $_;
    };
  }
} # _where_bad_value

sub _where_empty : Test(1) {
  dies_here_ok {
    where {};
  };
} # _where_empty

sub _where_named : Test(25) {
  for (
    [[' '] => [' ', []]],
    [['hoge fuga'] => ['hoge fuga', []]],
    [['hoge :fuga', fuga => 'abc'] => ['hoge ?', ['abc']]],
    [['hoge :fuga :fuga', fuga => 'abc'] => ['hoge ? ?', ['abc', 'abc']]],
    [['hoge :fuga:fuga', fuga => 'abc'] => ['hoge ?', ['abc']]],
    [['hoge :fug_a', fug_a => 'abc'] => ['hoge ?', ['abc']]],
    [['hoge :fuga :foo', fuga => 'abc', foo => 124]
         => ['hoge ? ?', ['abc', '124']]],
    [['hoge fuga ?'] => ['hoge fuga ?', []]],
    [['hoge fuga = ?', fuga => 'abc'] => ['hoge fuga = ? ', ['abc']]],
    [['hoge fuga = ?', fuga => "\x{500}"] => ['hoge fuga = ? ', ["\x{500}"]]],
    [['hoge fuga = ?', fuga => encode 'utf-8', "\x{500}"]
         => ['hoge fuga = ? ', [encode 'utf-8', "\x{500}"]]],
    [['hoge fu_ga = ?', fu_ga => 'abc'] => ['hoge fu_ga = ? ', ['abc']]],
    [['hoge `fuga` = ?', fuga => 'abc'] => ['hoge `fuga` = ? ', ['abc']]],
    [['hoge fuga < ?', fuga => 151] => ['hoge fuga < ? ', [151]]],
    [['hoge fuga <= ?', fuga => 151] => ['hoge fuga <= ? ', [151]]],
    [['hoge fuga > ?', fuga => 151] => ['hoge fuga > ? ', [151]]],
    [['hoge fuga >= ?', fuga => 151] => ['hoge fuga >= ? ', [151]]],
    [['hoge fuga <> ?', fuga => 151] => ['hoge fuga <> ? ', [151]]],
    [['hoge fuga != ?', fuga => 151] => ['hoge fuga != ? ', [151]]],
    [['hoge fuga <=> ?', fuga => 151] => ['hoge fuga <=> ? ', [151]]],
    [['foo=?and (bar=?)', foo => 1, bar => 2]
         => ['foo=? and (bar=? )', [1, 2]]],
    [['fuga IN (:foo)', foo => [1]] => ['fuga IN (?)', [1]]],
    [['fuga IN (:foo)', foo => [1, 2]] => ['fuga IN (?, ?)', [1, 2]]],
    [['fuga IN (:foo)', foo => [1, 2, 4]] => ['fuga IN (?, ?, ?)', [1, 2, 4]]],
    [['hoge = ?and', hoge => 'abc'] => ['hoge = ? and', ['abc']]],
  ) {
    eq_or_diff [where $_->[0]], $_->[1];
  }
} # _where_named

sub _where_named_not_specified : Test(2) {
  dies_here_ok { where [] };
  dies_here_ok { where [''] };
} # _where_named_not_specified

sub _where_named_not_defined : Test(6) {
  dies_here_ok { where [':hoge'] };
  dies_here_ok { where [':hoge', fuga => 1] };
  dies_here_ok { where ['hoge fuga = ?', fuga => undef] };
  dies_here_ok { where ['(:hoge)', hoge => [1, undef]] };
  dies_here_ok { where [':hoge' => 124] };
  dies_here_ok { where [':hoge and :foo', foo => (), hoge => 124] };
} # _where_named_not_defined

sub _where_named_ref : Test(9) {
  for (
    [':hoge', hoge => \undef],
    [':hoge', hoge => \'abc'],
    [':hoge', hoge => {foo => 'bar'}],
    [':hoge', hoge => bless [], 'test::hoge'],
    ['(:hoge)', hoge => [undef]],
    ['(:hoge)', hoge => [12, 44, \'']],
    ['(:hoge)', hoge => [12, 44, [foo => 443]]],
    ['(:hoge)', hoge => [12, 44, {foo => 52}]],
    ['(:hoge)', hoge => [12, 44, bless {}, 'test::hogexs']],
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_named_ref

sub _where_named_unused : Test(2) {
  dies_here_ok {
    where ['hoge', hoge => 123];
  };
  dies_here_ok {
    where ['hoge = :fuga', fuga => 123, foo => 51];
  };
} # _where_named_unused

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
