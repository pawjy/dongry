use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->execute ('create table foo1 (id int)', undef, source_name => 'ae')->then (sub {
    return $db->insert ('foo1', [{id => 1434}], source_name => 'ae');
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
    return $db->execute ('select * from foo1 ', undef, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a, [{id => 1434}];
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
} n => 14, name => 'insert promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->connect ('ae')->then (sub {
    return $db->execute ('create table foo2 (id int)', undef, source_name => 'ae');
  })->then (sub {
    return $db->insert ('foo2', [{id => 1434}], source_name => 'ae');
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
    return $db->execute ('select * from foo2 ', undef, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a, [{id => 1434}];
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
} n => 14, name => 'connect insert promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->execute ('create table foo3 (id int, primary key (id))', undef, source_name => 'ae')->then (sub {
    return $db->insert ('foo3', [{id => 1434}], source_name => 'ae');
  })->then (sub {
    return $db->insert ('foo3', [{id => 1434}], source_name => 'ae');
  })->then (sub {
    return $db->insert ('foo3', [{id => 1435}], source_name => 'ae');
  })->catch (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_sql, qr{INSERT};
      like $result->error_text, qr{[Dd]uplicate};
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
  })->then (sub {
    return $db->execute ('select * from foo3 ', undef, source_name => 'ae');
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
} n => 21, name => 'insert promise failure';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $invoked = 0;
  $db->execute ('create table foo4 (id int)', undef, source_name => 'ae')->then (sub {
    return $db->insert ('foo4', [{id => 1434}], source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      my $row = $result->first_as_row;
      isa_ok $row, 'Dongry::Table::Row';
      is $row->table_name, 'foo4';
    } $c;
  })->catch (sub {
    warn $_[0]->can ('error_text') ? $_[0]->error_text : $_[0];
    test { ok 0 } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 5, name => 'insert promise row';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $obj = {};
  $obj->{rand ()} = rand for 1..30000;

  my $invoked = 0;
  return Promise->resolve->then (sub {
    return $db->insert ('foo4', [$obj], source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      ok 0, $result;
    } $c;
  }, sub {
    my $e = $_[0];
    test {
      like $e, qr{^Too many values at \Q@{[__FILE__]}\E line @{[__LINE__-9]}};
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 1, name => 'insert large object';

RUN;

=head1 LICENSE

Copyright 2011-2022 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
