use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
BEGIN { $Test::Dongry::OldSQLMode = 1 }
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'root';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int unique key, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (22, 2)");
  $db->execute ("insert into foo (id, v1) values (32, 3)");

  my $result = $db->update
      ('foo', {id => my $id = \'id + 2', v1 => my $v1 = \'id * 2'},
       where => {id => 12});

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 0, v1 => 0, v2 => undef},
       {id => 22, v1 => 2, v2 => undef},
       {id => 32, v1 => 3, v2 => undef}];

  done $c;
} n => 1, name => '_update_values_by_not_sql';

RUN;

=head1 LICENSE

Copyright 2011-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
