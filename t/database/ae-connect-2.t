use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::X1;
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae', cb => sub {
    my ($db0, $result) = @_;
    test {
      is $db0, $db;
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
    $invoked++;
    $_[0]->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  });

  is $invoked, 0;
} n => 13, name => 'connect ae cb';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'hogefugaaaa', anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae', cb => sub {
    my ($db0, $result) = @_;
    test {
      is $db0, $db;
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{hogefugaaaa};
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $_[0]->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  });

  is $invoked, 0;
} n => 14, name => 'connect ae cb failure';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1}});

  $db->execute ('show tables', undef, source_name => 'master', cb => sub {
    $_[0]->disconnect (undef, cb => sub {
      test {
        ok 1;
        done $c;
        undef $c;
      } $c;
    });
  });
} n => 1, name => 'execute disconnect';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae')->then (sub {
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
    $invoked++;
    $db->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  }, sub {
    test {
      ok 0;
    } $c;
  })->catch (sub {
    warn "ERROR: $_[0]";
  });

  is $invoked, 0;
} n => 12, name => 'connect ae promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'hogefugaaaa', anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae')->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my ($result) = @_;
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{hogefugaaaa};
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $db->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  });

  is $invoked, 0;
} n => 13, name => 'connect ae failure promise';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'DBI:mysql:foo=bar', anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae')->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my ($result) = @_;
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{Unknown};
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $db->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  });

  is $invoked, 0;
} n => 13, name => 'connect ae failure promise, unknown param';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'DBI:your:host=bar.test', anyevent => 1}});

  my $invoked = 0;
  $db->connect ('ae')->then (sub {
    test {
      ok 0;
    } $c;
  }, sub {
    my ($result) = @_;
    test {
      isa_ok $result, 'Dongry::Database::Executed';
      ok not $result->is_success;
      ok $result->is_error;
      like $result->error_text, qr{Non-MySQL};
      dies_here_ok { $result->all };
      dies_here_ok { $result->each (sub { }) };
      dies_here_ok { $result->first };
      dies_here_ok { $result->all_as_rows };
      dies_here_ok { $result->each_as_row (sub { }) };
      dies_here_ok { $result->first_as_row };
    } $c;
    $invoked++;
    $db->disconnect ('ae', cb => sub {
      my ($db2) = @_;
      test {
        is $db2, $db;
        is $invoked, 1;
        done $c;
        undef $c;
      } $c;
    });
  });

  is $invoked, 0;
} n => 13, name => 'connect ae failure promise, non-mysql';

test {
  my $c = shift;
  my $db = Dongry::Database->new (sources => {async => {dsn => '', anyevent => 1}});
  my $result = $db->disconnect;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->can ('then');
  $result->then (sub {
    my $x = $_[0];
    test {
      is $x, undef;
      done $c;
      undef $c;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  });
} n => 3, name => 'disconnect async only';

test {
  my $c = shift;
  my $db = Dongry::Database->new (sources => {sync => {dsn => ''}, async => {dsn => '', anyevent => 1}});
  my $result = $db->disconnect;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->can ('then');
  $result->then (sub {
    my $x = $_[0];
    test {
      is $x, undef;
      done $c;
      undef $c;
    } $c;
  }, sub {
    test {
      ok 0;
    } $c;
  });
} n => 3, name => 'disconnect sync and async';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1}});

  $db->connect ('ae')->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      ok not $db->{dbhs}->{ae};
      done $c;
      undef $c;
    } $c;
  });
} n => 1, name => 'connect then disconnect';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1},
                   ae2 => {dsn => $dsn, anyevent => 1}});

  $db->connect ('ae')->then (sub {
    return $db->connect ('ae2');
  })->then (sub {
    test {
      ok $db->{dbhs}->{ae};
      ok $db->{dbhs}->{ae2};
    } $c;
    return $db->disconnect ('ae');
  })->then (sub {
    return $db->disconnect ('ae2');
  })->then (sub {
    test {
      ok not $db->{dbhs}->{ae};
      ok not $db->{dbhs}->{ae2};
      done $c;
      undef $c;
    } $c;
  });
} n => 4, name => 'connect then disconnect, multiple';

run_tests;

=head1 LICENSE

Copyright 2011-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
