package test::Dongry::SQL::where;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL qw(where);
use Dongry::Type::DateTime;
use Encode;
use Data::Dumper;

$Dongry::SQL::SortKeys = 1;

sub _where_valid_hashref : Test(51) {
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
      [{bar => 160, foo => 120} => ['`bar` = ? AND `foo` = ?', [160, 120]]],
      [{foo => 'bar', 'baz' => {-not => undef}}
           => ['`baz` IS NOT NULL AND `foo` = ?', ['bar']]],
      [{foo => 'bar', 'baz' => {-not => undef}, 'ab `c)' => {-lt => 120}}
           => ['`ab ``c)` < ? AND `baz` IS NOT NULL AND `foo` = ?',
               [120, 'bar']]],
      [{foo => {-le => 120, -gt => 89}}
           => ['`foo` > ? AND `foo` <= ?', [89, 120]]],
      [{foo => {-le => 120, -in => [10, 20]}}
           => ['`foo` IN (?, ?) AND `foo` <= ?', [10, 20, 120]]],
  ) {
    eq_or_diff [where ($_->[0])], $_->[1];
  }
} # _where_valid_hashref

sub _where_no_sort : Test(5) {
  local $Dongry::SQL::SortKeys = 0;
  for (
    [{foo => 12, bar => 23} =>
     [
       ['`foo` = ? AND `bar` = ?', [12, 23]],
       ['`bar` = ? AND `foo` = ?', [23, 12]],
     ]],
    [{foo => 50, bar => 23} =>
     [
       ['`foo` = ? AND `bar` = ?', [50, 23]],
       ['`bar` = ? AND `foo` = ?', [23, 50]],
     ]],
    [{foo => {-lt => 50}, bar => 23} =>
     [
       ['`foo` < ? AND `bar` = ?', [50, 23]],
       ['`bar` = ? AND `foo` < ?', [23, 50]],
     ]],
    [{abc => {-lt => 50}, bar => 23} =>
     [
       ['`abc` < ? AND `bar` = ?', [50, 23]],
       ['`bar` = ? AND `abc` < ?', [23, 50]],
     ]],
    [{foo => {-lt => 50, -gt => 20}, bar => 23} =>
     [
       ['`foo` < ? AND `foo` > ? AND `bar` = ?', [50, 20, 23]],
       ['`foo` > ? AND `foo` < ? AND `bar` = ?', [20, 50, 23]],
       ['`bar` = ? AND `foo` < ? AND `foo` > ?', [23, 50, 20]],
       ['`bar` = ? AND `foo` > ? AND `foo` < ?', [23, 20, 50]],
     ]],
  ) {
    my $got = Dumper [where ($_->[0])];
    my @expected = map { Dumper $_ } @{$_->[1]};
    ok ((grep { $got eq $_ } @expected), $got);
  }
} # _where_no_sort

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
    [['hoge :fuga:', fuga => 'abc'] => ['hoge ?:', ['abc']]],
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

sub _where_named_not_defined : Test(8) {
  dies_here_ok { where [':hoge'] };
  dies_here_ok { where [':hoge', fuga => 1] };
  dies_here_ok { where ['hoge fuga = ?', fuga => undef] };
  dies_here_ok { where ['(:hoge)', hoge => [1, undef]] };
  dies_here_ok { where [':hoge' => 124] };
  dies_here_ok { where [':hoge and :foo', foo => (), hoge => 124] };
  dies_here_ok { where [':hoge:id', hoge => undef] };
  dies_here_ok { where [':hoge:id', 'hoge:id' => 333] };
} # _where_named_not_defined

sub _where_named_ref : Test(10) {
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
    [':hoge:id', hoge => \undef],
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

sub _where_named_id : Test(7) {
  for (
    [[':foo:id = 124', foo => ''] => ['`` = 124', []]],
    [[':foo:id = 124', foo => '123'] => ['`123` = 124', []]],
    [[':foo:id = 124', foo => 'abc'] => ['`abc` = 124', []]],
    [[':foo:id = 124', foo => '`\\%'] => ['```\\%` = 124', []]],
    [[':foo:id = 124', foo => "\x{5001}"] => ["`\x{5001}` = 124", []]],
    [[':foo:id = 124', foo => encode 'utf-8', "\x{5001}"]
         => [(encode 'utf-8', "`\x{5001}` = 124"), []]],
    [[':foo:id = :foo', foo => '`\\%'] => ['```\\%` = ?', ['`\\%']]],
  ) {
    eq_or_diff [where $_->[0]], $_->[1];
  }
} # _where_named_id

sub _where_named_unknown_instruction : Test(1) {
  dies_here_ok {
    where ['hoge :fuga:fuga', fuga => 'abc'];
  };
} # _where_named_unknown_instruction

sub _where_named_sub : Test(3) {
  for (
    [['foo = ? AND :bar:sub',
      foo => 1254,
      bar => {hoge => 1, fuga => {'!=', 2}}]
         => ['foo = ? AND (`fuga` != ? AND `hoge` = ?)', [1254, 2, 1]]],
    [['foo = ? AND :bar:optsub',
      foo => 1254,
      bar => {hoge => 1, fuga => {'!=', 2}}]
         => ['foo = ? AND (`fuga` != ? AND `hoge` = ?)', [1254, 2, 1]]],
    [['foo = ? AND :bar:optsub', foo => 1254, bar => {}]
         => ['foo = ? AND (1 = 1)', [1254]]],
  ) {
    eq_or_diff [where $_->[0]], $_->[1];
  }
} # _where_named_sub

sub _where_named_sub_bad_value : Test(17) {
  for (
    [':foo:sub' => undef],
    [':foo:sub' => 'abc'],
    [':foo:sub' => \'xyz'],
    [':foo:sub' => []],
    [':foo:sub' => [foo => 124]],
    [':foo:sub' => ['hoge => :abc', abc => 123]],
    [':foo:sub' => {}],
    [':foo:sub' => bless {}, 'test:foo'],
    [':foo:sub' => {foo => {-unknown => 12}}],
    [':foo:optsub' => undef],
    [':foo:optsub' => 'abc'],
    [':foo:optsub' => \'xyz'],
    [':foo:optsub' => []],
    [':foo:optsub' => [foo => 124]],
    [':foo:optsub' => ['hoge => :abc', abc => 123]],
    [':foo:optsub' => bless {}, 'test:foo'],
    [':foo:optsub' => {foo => {-unknown => 12}}],
  ) {
    dies_here_ok {
      where $_;
    };
  }
} # _where_named_sub_bad_value

sub _where_bad_values : Test(8) {
  dies_here_ok { where undef };
  dies_here_ok { where '' };
  dies_here_ok { where 0 };
  dies_here_ok { where 'foo' };
  dies_here_ok { where \'foo' };
  dies_here_ok { where bless {}, 'test::hoge' };
  dies_here_ok { where bless [], 'test::foo' };
  dies_here_ok { where bless [], 'List::Rubyish' };
} # _where_bad_values

sub _where_hashref_parsed : Test(17) {
  for (
    [{foo => undef},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` = ?', ['0000-00-00 00:00:00']]],
    [{foo => DateTime->new (year => 2001, month => 12, day => 3)},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` = ?', ['2001-12-03 00:00:00']]],
    [{foo => {-eq => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` = ?', ['2001-12-03 00:00:00']]],
    [{foo => {-ne => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` != ?', ['2001-12-03 00:00:00']]],
    [{foo => {-lt => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` < ?', ['2001-12-03 00:00:00']]],
    [{foo => {-le => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` <= ?', ['2001-12-03 00:00:00']]],
    [{foo => {-gt => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` > ?', ['2001-12-03 00:00:00']]],
    [{foo => {-ge => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` >= ?', ['2001-12-03 00:00:00']]],
    [{foo => {-like => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` LIKE ?', ['2001-12-03 00:00:00']]],
    [{foo => {-prefix => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` LIKE ?', ['2001-12-03 00:00:00%']]],
    [{foo => {-suffix => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` LIKE ?', ['%2001-12-03 00:00:00']]],
    [{foo => {-infix => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` LIKE ?', ['%2001-12-03 00:00:00%']]],
    [{foo => {-regexp => DateTime->new (year => 2001, month => 12, day => 3)}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` REGEXP ?', ['2001-12-03 00:00:00']]],
    [{foo => {-in => [DateTime->new (year => 2001, month => 12, day => 3),
                      DateTime->new (year => 2010, month => 10, day => 21)]}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` IN (?, ?)', ['2001-12-03 00:00:00', '2010-10-21 00:00:00']]],
    [{foo => {-in => [DateTime->new (year => 2001, month => 12, day => 3),
                      DateTime->new (year => 2010, month => 10, day => 21),
                      undef]}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` IN (?, ?, ?)', ['2001-12-03 00:00:00',
                             '2010-10-21 00:00:00',
                             '0000-00-00 00:00:00']]],
    [{foo => {-lt => undef}},
     {type => {foo => 'timestamp_as_DateTime'}},
     ['`foo` < ?', ['0000-00-00 00:00:00']]],
    [['1 AND :hoge:sub',
      hoge => {foo => DateTime->new (year => 2001, month => 12, day => 3)}],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['1 AND (`foo` = ?)', ['2001-12-03 00:00:00']]],
  ) {
    eq_or_diff [where $_->[0], $_->[1]] => $_->[2];
  }
} # _where_hashref_parsed

sub _where_hashref_unknown_type : Test(4) {
  dies_here_ok {
    where {foo => 'abc'}, {type => {foo => 'as_unknown'}};
  };
  dies_here_ok {
    where {foo => {-ne => 'abc'}}, {type => {foo => 'as_unknown'}};
  };
  dies_here_ok {
    where {foo => {-in => ['abc']}}, {type => {foo => 'as_unknown'}};
  };
  dies_here_ok {
    where [':hoge:sub', hoge => {foo => {-in => ['abc']}}],
        {type => {foo => 'as_unknown'}};
  };
} # _where_hashref_unknown_type

sub _where_hashref_type_error : Test(3) {
  dies_ok {
    where {foo => 'abc def'}, {type => {foo => 'timestamp_as_DateTime'}};
  };
  dies_ok {
    where {foo => {-eq => 'abc def'}},
        {type => {foo => 'timestamp_as_DateTime'}};
  };
  dies_ok {
    where {foo => {-in => ['abc def']}},
        {type => {foo => 'timestamp_as_DateTime'}};
  };
} # _where_hashref_type_error

sub _where_named_parsed : Test(6) {
  for (
    [['foo = :foo',
      foo => DateTime->new (year => 2001, month => 12, day => 3)],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['foo = ?', ['2001-12-03 00:00:00']]],
    [['foo = :foo', foo => undef],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['foo = ?', ['0000-00-00 00:00:00']]],
    [['foo IN (:foo)',
      foo => [DateTime->new (year => 2001, month => 12, day => 3), undef]],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['foo IN (?, ?)', ['2001-12-03 00:00:00', '0000-00-00 00:00:00']]],
    [['foo = :bar::foo', bar => undef],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['foo = ?', ['0000-00-00 00:00:00']]],
    [['foo IN (:bar::foo)',
      bar => [DateTime->new (year => 2001, month => 12, day => 3), undef]],
     {type => {foo => 'timestamp_as_DateTime'}},
     ['foo IN (?, ?)', ['2001-12-03 00:00:00', '0000-00-00 00:00:00']]],
    [[':hoge1::hoge < foo AND foo < :hoge2::hoge',
      hoge1 => DateTime->new (year => 2001, month => 10, day => 3),
      hoge2 => DateTime->new (year => 2010, month => 10, day => 2)],
     {type => {hoge => 'timestamp_as_DateTime', foo => 'as_unknown'}},
     ['? < foo AND foo < ?', ['2001-10-03 00:00:00', '2010-10-02 00:00:00']]],
  ) {
    eq_or_diff [where $_->[0], $_->[1]] => $_->[2];
  }
} # _where_hashref_parsed

sub _where_named_parsed_type_error : Test(3) {
  dies_ok {
    where ['foo = :foo', foo => 'abc'],
        {type => {foo => 'timestamp_as_DateTime'}};
  };
  dies_ok {
    where ['foo = :foo', foo => {-lt => 'abc'}],
        {type => {foo => 'timestamp_as_DateTime'}};
  };
  dies_ok {
    where ['foo IN (:foo)', foo => ['abc']],
        {type => {foo => 'timestamp_as_DateTime'}};
  };
} # _where_named_parsed_type_error

sub _where_no_modification : Test(1) {
  my $where = {foo => 'bar', hoge => {-lt => 120}};
  where $where;
  eq_or_diff $where, {foo => 'bar', hoge => {-lt => 120}};
} # _where_no_modification

sub _where_no_modification_parsed : Test(1) {
  my $where = {foo => 'bar', hoge => {-lt => \120}};
  where $where, {type => {hoge => 'as_ref'}};
  eq_or_diff $where, {foo => 'bar', hoge => {-lt => \120}};
} # _where_no_modification_parsed

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
