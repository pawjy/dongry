package test::Dongry::SQL::order;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL;

sub _order : Test(15) {
  for (
    [undef, ''],
    ['' => ''],
    [[] => ''],
    [['fioi'] => '`fioi` ASC'],
    [[fioi => 1] => '`fioi` ASC'],
    [[fioi => '+1'] => '`fioi` ASC'],
    [[fioi => 'ASC'] => '`fioi` ASC'],
    [[fioi => 'asc'] => '`fioi` ASC'],
    [[fioi => -1] => '`fioi` DESC'],
    [[fioi => 'DESC'] => '`fioi` DESC'],
    [[fioi => 'desc'] => '`fioi` DESC'],
    [[fioi => 'asc', abc => -1] => '`fioi` ASC, `abc` DESC'],
    [['123 `abc\\' => 1] => '`123 ``abc\` ASC'],
    [['' => -1] => '`` DESC'],
    [[abc => 1, abc => -1] => '`abc` ASC, `abc` DESC'],
  ) {
    eq_or_diff order $_->[0], $_->[1];
  }
} # _order

sub _order_bad : Test(3) {
  for (
    [fioi => 'hoge'],
    [fioi => 'foo', abc => -1],
    [xyz => 1, fioi => 'foo', abc => -1],
  ) {
    dies_here_ok {
      order $_;
    };
  }
} # _order_bad

sub _reverse_order_struct : Test(15) {
  for (
    [undef, undef],
    ['' => []],
    [[] => []],
    [['fioi'] => [fioi => -1]],
    [[fioi => 1] => [fioi => -1]],
    [[fioi => '+1'] => [fioi => -1]],
    [[fioi => 'ASC'] => [fioi => -1]],
    [[fioi => 'asc'] => [fioi => -1]],
    [[fioi => -1] => [fioi => 1]],
    [[fioi => 'DESC'] => [fioi => 1]],
    [[fioi => 'desc'] => [fioi => 1]],
    [[fioi => 'asc', abc => -1] => [fioi => -1, abc => 1]],
    [['123 `abc\\' => 1] => ['123 `abc\\' => -1]],
    [['' => -1] => ['' => 1]],
    [[foo => 1, 'bar'] => [foo => -1, bar => -1]],
  ) {
    eq_or_diff reverse_order_struct $_->[0], $_->[1];
  }
} # _reverse_order_struct

sub _reverse_order_struct_non_destructive : Test(2) {
  my $order = [foo => 1, bar => -1];
  eq_or_diff reverse_order_struct $order, [foo => -1, bar => 1];
  eq_or_diff $order, [foo => 1, bar => -1];
} # _reverse_order_struct_non_destructive

sub _reverse_order_struct_bad : Test(3) {
  for (
    [fioi => 'hoge'],
    [fioi => 'foo', abc => -1],
    [xyz => 1, fioi => 'foo', abc => -1],
  ) {
    dies_here_ok {
      reverse_order_struct $_;
    };
  }
} # _reverse_order_struct_bad

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
