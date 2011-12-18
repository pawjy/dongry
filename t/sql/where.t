package test::Dongry::SQL::where;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL;

$Dongry::SQL::SortKeys = 1;

sub _where_valid_hashref : Test(35) {
  for (
      [{} => [undef, []]],
      [{foo => undef} => ['`foo` IS NULL', []]],
      [{foo => 'bar'} => ['`foo` = ?', ['bar']]],
      [{foo => {-eq => undef}} => ['`foo` IS NULL', []]],
      [{foo => {-eq => 'bar'}} => ['`foo` = ?', ['bar']]],
      [{foo => {'==' => undef}} => ['`foo` IS NULL', []]],
      [{foo => {'==' => 'bar'}} => ['`foo` = ?', ['bar']]],
      [{foo => {-ne => undef}} => ['`foo` IS NOT NULL', []]],
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
      [{foo => {-prefix => 'b\%a_a'}} => ['`foo` LIKE ?', ['b\\\\\\%a\\_a%']]],
      [{foo => {-suffix => 'b\%a_a'}} => ['`foo` LIKE ?', ['%b\\\\\\%a\\_a']]],
      [{foo => {-infix => 'b\%a_a'}} => ['`foo` LIKE ?', ['%b\\\\\\%a\\_a%']]],
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

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
