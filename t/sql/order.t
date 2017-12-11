use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/lib');
use StandaloneTests;
use Dongry::SQL;

{
  for my $v (
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
    test {
      my $c = shift;
      is_deeply order $v->[0], $v->[1];
      done $c;
    } n => 1, name => ['order', $v->[1]];
  }
}

for my $v (
  [fioi => 'hoge'],
  [fioi => 'foo', abc => -1],
  [xyz => 1, fioi => 'foo', abc => -1],
) {
  test {
    my $c = shift;
    eval {
      order $v;
    };
    ok $@;
    done $c;
  } n => 1, name => 'order bad';
}

{
  for my $v (
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
    test {
      my $c = shift;
      is_deeply reverse_order_struct $v->[0], $v->[1];
      done $c;
    } n => 1, name => 'reverse order struct';
  }
}

test {
  my $c = shift;
  my $order = [foo => 1, bar => -1];
  is_deeply reverse_order_struct $order, [foo => -1, bar => 1];
  is_deeply $order, [foo => 1, bar => -1];
  done $c;
} n => 2, name => 'reverse_order_struct_non_destructive';

for my $v (
  [fioi => 'hoge'],
  [fioi => 'foo', abc => -1],
  [xyz => 1, fioi => 'foo', abc => -1],
) {
  test {
    my $c = shift;
    eval {
      reverse_order_struct $v;
    };
    ok $@;
    done $c;
  } n => 1, name => 'reverse_order_struct_bad';
}

RUN;

=head1 LICENSE

Copyright 2011-2017 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
