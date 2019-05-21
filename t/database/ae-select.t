use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';
my $dsn2 = test_dsn 'hoge2';
my $dsn3 = test_dsn 'hoge3';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->execute ('create table foo1 (id int)', undef, source_name => 'ae')->then (sub {
    return $db->insert ('foo1', [{id => 1434}], source_name => 'ae');
  })->then (sub {
    return $db->select ('foo1', {id => 1434}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a, [{id => 1434}];
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
  })->catch (sub {
    warn $_[0]->can ('error_text') ? $_[0]->error_text : $_[0];
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 10, name => 'select promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->execute ('create table foo2 (id int)', undef, source_name => 'ae')->then (sub {
    return $db->insert ('foo2', [{id => 1434}], source_name => 'ae');
  })->then (sub {
    return $db->select ('foo2', {id => 1434}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      my $row = $result->all_as_rows->to_a->[0];
      isa_ok $row, 'Dongry::Table::Row';
      is $row->table_name, 'foo2';
      is $row->get ('id'), 1434;
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
  })->catch (sub {
    warn $_[0]->can ('error_text') ? $_[0]->error_text : $_[0];
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 12, name => 'select promise row';

test {
  my $c = shift;
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1, anyevent => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1, anyevent => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1, anyevent => 1}});

  my $db;
  return Promise->all ([
    $db1->execute ('create table foo3 (id int)'),
    $db2->execute ('create table foo3 (id int)'),
    $db3->execute ('create table foo3 (id int)'),
    $db1->execute ('insert into foo3 (id) values (1)'),
    $db2->execute ('insert into foo3 (id) values (2)'),
    $db3->execute ('insert into foo3 (id) values (3)'),
  ])->then (sub {
    
    $db = Dongry::Database->new
        (sources => {default => {dsn => $dsn, anyevent => 1},
                     master => {dsn => $dsn2, anyevent => 1},
                     heavy => {dsn => $dsn3, anyevent => 1}},
         master_only => 1);

    return $db->select ('foo3', {id => {'>=', 0}});
  })->then (sub {
    my $result1 = $_[0];
    test {
      is $result1->first->{id}, 2;
    } $c;
    
    return $db->select ('foo3', {id => {'>=', 0}},
                        source_name => 'default');
  })->then (sub {
    my $result2 = $_[0];
    test {
      is $result2->first->{id}, 1;
    } $c;
    
    return $db->select ('foo3', {id => {'>=', 0}},
                        source_name => 'master');
  })->then (sub {
    my $result3 = $_[0];
    test {
      is $result3->first->{id}, 2;
    } $c;

    return $db->select ('foo3', {id => {'>=', 0}},
                        source_name => 'heavy');
  })->then (sub {
    my $result4 = $_[0];
    test {
      is $result4->first->{id}, 3;
    } $c;
    
    return $db->select ('foo3', {id => {'>=', 0}},
                        source_name => 'notfound');
  })->then (sub { test { ok 0 } $c }, sub {
    my $e = $_[0];
    test {
      ok $c, $c;
    } $c;
  })->then (sub {
    done $c;
    undef $c,
  });
} n => 5, name => 'select master only';

test {
  my $c = shift;
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1, anyevent => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1, anyevent => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1, anyevent => 1}});

  my $db;
  return Promise->all ([
    $db1->execute ('create table foo4 (id int)'),
    $db2->execute ('create table foo4 (id int)'),
    $db3->execute ('create table foo4 (id int)'),
    $db1->execute ('insert into foo4 (id) values (1)'),
    $db2->execute ('insert into foo4 (id) values (2)'),
    $db3->execute ('insert into foo4 (id) values (3)'),
  ])->then (sub {
    
    $db = Dongry::Database->new
        (sources => {default => {dsn => $dsn, anyevent => 1},
                     master => {dsn => $dsn2, anyevent => 1},
                     heavy => {dsn => $dsn3, anyevent => 1}},
         master_only => 0);

    return $db->select ('foo4', {id => {'>=', 0}});
  })->then (sub {
    my $result1 = $_[0];
    test {
      is $result1->first->{id}, 1;
    } $c;
    
    return $db->select ('foo4', {id => {'>=', 0}},
                        source_name => 'default');
  })->then (sub {
    my $result2 = $_[0];
    test {
      is $result2->first->{id}, 1;
    } $c;
    
    return $db->select ('foo4', {id => {'>=', 0}},
                        source_name => 'master');
  })->then (sub {
    my $result3 = $_[0];
    test {
      is $result3->first->{id}, 2;
    } $c;

    return $db->select ('foo4', {id => {'>=', 0}},
                        source_name => 'heavy');
  })->then (sub {
    my $result4 = $_[0];
    test {
      is $result4->first->{id}, 3;
    } $c;
    
    return $db->select ('foo4', {id => {'>=', 0}},
                        source_name => 'notfound');
  })->then (sub { test { ok 0 } $c }, sub {
    my $e = $_[0];
    test {
      ok $c, $c;
    } $c;
  })->then (sub {
    done $c;
    undef $c,
  });
} n => 5, name => 'select master only false';

RUN;

=head1 LICENSE

Copyright 2011-2019 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
