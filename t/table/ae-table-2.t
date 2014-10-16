use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::X1;
use Test::Dongry;
use Dongry::Database;
use Dongry::Type::Time;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo1 => {type => {value => 'timestamp'}}});

  $db->execute ('create table foo1 (id int, value datetime)', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo1')->insert
        ([{id => 123, value => 5325333}, {id => 52, value => 5113}],
         source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a,
          [{id => 123, value => '1970-03-03 15:15:33'}, {id => 52, value => '1970-01-01 01:25:13'}];
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    return $db->execute ('select * from foo1 order by id asc', undef, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a,
          [{id => 52, value => '1970-01-01 01:25:13'}, {id => 123, value => '1970-03-03 15:15:33'}];
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
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo11 => {type => {value => 'timestamp'}}});

  $db->execute ('create table foo11 (id int, value datetime, primary key (id))', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo11')->create
        ({id => 123, value => 5325333}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Table::Row';
      eq_or_diff $result->values_as_hashref,
          [{id => 123, value => '1970-03-03 15:15:33'}];
    } $c;
    return $db->table ('foo11')->create
        ({id => 123, value => 5325333}, duplicate => 'ignore', source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, undef;
    } $c;
    return $db->execute ('select * from foo11 order by id asc', undef, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      eq_or_diff $result->all->to_a,
          [{id => 123, value => '1970-03-03 15:15:33'}];
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
} n => 7, name => 'create promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo2 => {type => {value => 'timestamp'}}});

  $db->execute ('create table foo2 (id int, value datetime)', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo2')->insert
        ([{id => 123, value => 515333333}, {id => 52, value => 532333444}],
         source_name => 'ae');
  })->then (sub {
    return $db->table ('foo2')->find ({id => 123}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Table::Row';
      eq_or_diff $result->values_as_hashref,
          {id => 123, value => '1986-05-01 12:08:53'};
    } $c;
  })->then (sub {
    return $db->table ('foo2')->find_all ({id => {-not => undef}}, order => [id => 'ASC'], source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'List::Ish';
      eq_or_diff $result->map (sub { $_->values_as_hashref })->to_a,
          [{id => 52, value => '1986-11-14 06:24:04'}, {id => 123, value => '1986-05-01 12:08:53'}];
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
} n => 4, name => 'find promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo21 => {type => {value => 'timestamp'}}});

  $db->execute ('create table foo21 (id int, value datetime)', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo21')->insert
        ([{id => 123, value => 515333333}, {id => 52, value => 532333444}],
         source_name => 'ae');
  })->then (sub {
    return $db->table ('foo21')->find ({idx => 123}, source_name => 'ae');
  })->then (sub {
    test { ok 0 } $c;
  }, sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{idx};
    } $c;
  })->then (sub {
    return $db->table ('foo21')->find_all ({idx => {-not => undef}}, order => [id => 'ASC'], source_name => 'ae');
  })->then (sub {
    test { ok 0 } $c;
  }, sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{idx};
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
} n => 8, name => 'find promise error';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo3 => {type => {value => 'timestamp'},
                           primary_keys => ['id']}});

  $db->execute ('create table foo3 (id int, value datetime)', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo3')->insert
        ([{id => 123, value => 515333333}, {id => 52, value => 532333444}],
         source_name => 'ae');
  })->then (sub {
    return $db->table ('foo3')->find ({id => 123}, source_name => 'ae')->then (sub {
      return $_[0]->update ({value => 125555}, source_name => 'ae');
    });
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
  })->then (sub {
    return $db->table ('foo3')->find ({id => 123}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      eq_or_diff $result->values_as_hashref,
          {id => 123, value => '1970-01-02 10:52:35'};
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
} n => 10, name => 'update promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}},
       schema => {foo4 => {type => {value => 'timestamp'},
                           primary_keys => ['id']}});

  $db->execute ('create table foo4 (id int, value datetime)', undef, source_name => 'ae')->then (sub {
    return $db->table ('foo4')->insert
        ([{id => 123, value => 515333333}, {id => 52, value => 532333444}],
         source_name => 'ae');
  })->then (sub {
    return $db->table ('foo4')->find ({id => 123}, source_name => 'ae')->then (sub {
      return $_[0]->delete (source_name => 'ae');
    });
  })->then (sub {
    my $result = $_[0];
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok $result->is_success;
      ok not $result->is_error;
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
  })->then (sub {
    return $db->table ('foo4')->find ({id => 123}, source_name => 'ae');
  })->then (sub {
    my $result = $_[0];
    test {
      is $result, undef;
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
} n => 10, name => 'delete promise';

run_tests;

=head1 LICENSE

Copyright 2011-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
