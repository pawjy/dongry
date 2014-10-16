package test::Dongry::Database::anyevent::execute;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _execute_cb_all : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 1}, {id => 2}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_all

sub _execute_cb_first : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 2;
  eq_or_diff $result->first, {id => 1};
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_first

sub _execute_cb_each : Test(15) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  my @values;
  dies_here_ok { $result->each (sub { push @values, $_ }) };
  eq_or_diff \@values, [];
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_each

sub _execute_cb_all_as_rows : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 2;
  dies_here_ok { $result->all_as_rows };
  isa_list_ok $result->all;
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_all_as_rows

sub _execute_cb_each_cb : Test(17) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my @row;
  $db->execute ('select * from foo order by id asc', undef,
                each_cb => sub {
                  push @row, $_;
                },
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  push @row, undef;
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  eq_or_diff \@row, [
    {id => 1},
    {id => 2},
    undef,
  ];

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  dies_here_ok { $result->all };
  my @values;
  dies_here_ok { $result->each (sub { push @values, $_ }) };
  eq_or_diff \@values, [];
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_each_cb

sub _execute_cb_each_as_row_cb : Test(23) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my @row;
  $db->execute ('select * from foo order by id asc', undef,
                each_as_row_cb => sub {
                  push @row, $_;
                },
                table_name => 'foobar',
                each_cb => sub {
                  ok 0;
                },
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  push @row, undef;
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $row[0], 'Dongry::Table::Row';
  is $row[0]->table_name, 'foobar';
  is $row[0]->get ('id'), 1;
  isa_ok $row[1], 'Dongry::Table::Row';
  is $row[1]->table_name, 'foobar';
  is $row[1]->get ('id'), 2;
  is $row[2], undef;

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 0;
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  dies_here_ok { $result->all };
  my @values;
  dies_here_ok { $result->each (sub { push @values, $_ }) };
  eq_or_diff \@values, [];
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_each_as_row_cb

sub _execute_cb_each_as_row_cb_no_table_name : Test(3) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my @row;
  dies_here_ok {
    $db->execute ('select * from foo order by id asc', undef,
                  each_as_row_cb => sub {
                    is $_[0], $db;
                    push @row, $_[1];
                  },
                  each_cb => sub {
                    ok 0;
                  },
                  cb => sub {
                    is $_[0], $db;
                    $result = $_[1];
                    push @row, undef;
                    $cv->send;
                  },
                  source_name => 'ae');
  };

  eq_or_diff \@row, [];
  is $result, undef;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_each_as_row_cb_no_table_name

sub _execute_cb_first_as_row : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $error;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 2;
  dies_here_ok { $result->first_as_row };
  isa_list_ok $result->all;
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_first_as_row

sub _execute_cb_each_as_row : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $error;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  my $invoked;
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  isa_list_ok $result->all;
  dies_here_ok { $result->first };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_each_as_row

sub _execute_syntax_error : Test(7) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $db->execute ('select * from', undef,
                cb => sub {
                  $invoked++;
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{syntax};
  is $result->error_sql, 'select * from';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_syntax_error

sub _execute_syntax_error_onerror : Test(8) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $onerror;
  my $onerror_info;
  $db->onerror (sub {
    my ($self, %args) = @_;
    is $self, $db;
    $onerror_info = \%args;
    $onerror++;
  });

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from', undef,
                cb => sub {
                  $cv->send;
                },
                source_name => 'ae');
  my $execute_line = __LINE__ - 1;

  $cv->recv;

  is $onerror, 1;
  like $onerror_info->{text}, qr{syntax};
  is $onerror_info->{sql}, 'select * from';
  is $onerror_info->{file_name}, __FILE__;
  is $onerror_info->{line}, $execute_line;
  ok $onerror_info->{anyevent};
  is $onerror_info->{source_name}, 'ae';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_syntax_error_onerror

sub _execute_syntax_error_followed : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $cv->begin;
  $db->execute ('select * from', undef, source_name => 'ae',
                cb => sub { $cv->end }, onerror => sub { $cv->end });

  my $invoked;
  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'ae',
                cb => sub { 
                  $invoked++;
                  is $_[0], $db;
                  $cv->end;
                });

  $cv->end;
  $cv->recv;

  is $invoked, 1;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_syntax_error_followed

sub _execute_syntax_error_followed_2 : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $invoked;
  $db->execute ('select * from', undef, source_name => 'ae',
                cb => sub {
                  return if $_[1]->is_success;
                  $db->execute ('select * from foo', undef,
                                source_name => 'ae',
                                cb => sub {
                                  $invoked++;
                                  is $_[0], $db;
                                  $cv->send;
                                });
                });

  $cv->recv;

  is $invoked, 1;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_syntax_error_followed_2

sub _execute_connection_error : Test(7) {
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'dbi:mysql:foo..xbar', anyevent => 1,
                          writable => 1}});
  
  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  $db->execute ('create table foo (id int)', undef,
                source_name => 'ae',
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $invoked++;
                  $cv->send;
                });

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr[Can't connect|Unknown database|Access denied|\|connect\| failed];
  is $result->error_sql, 'create table foo (id int)';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_connection_error

sub _execute_connection_error_2 : Test(12) {
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'dsi:mysql:foo:bar', anyevent => 1,
                          writable => 1}});
  
  my $cv = AnyEvent->condvar;
  
  my $invoked;
  my $result;
  $db->execute ('create table foo (id int)', undef,
                source_name => 'ae',
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $invoked++;
                  $cv->send;
                });

  $cv->recv;

  isa_ok $db->{dbhs}->{ae}, 'Dongry::Database::BrokenConnection';

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{Can't connect|Unknown database|Access denied|\|connect\| failed};
  is $result->error_sql, 'create table foo (id int)';

  $result = undef;
  $cv = AnyEvent->condvar;

  $db->execute ('hoge', undef, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');
  $cv->recv;
  
  is $invoked, 2;
  like $result->error_text, qr{Can't connect|Unknown database|Access denied|\|connect\| failed};
  is $result->error_sql, 'hoge';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_connection_error_2

sub _execute_return_value : Test(9) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (3)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result = $db->execute ('select * from foo', undef,
                             cb => sub { is $_[1]->row_count, 2; $cv->send },
                             source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->each (sub { }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { }) };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_return_value

sub _execute_cb_croak : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  Carp::croak "hoge";
                },
                source_name => 'ae');

  $db->execute ('select * from foo', undef, cb => sub {
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;
  ok not $@;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_croak

sub _execute_cb_die : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  die "hoge";
                },
                source_name => 'ae');

  $db->execute ('select * from foo', undef, cb => sub {
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;
  ok not $@;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_die

sub _execute_cb_die_by_error : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('select * from foo order by id asc', undef,
                cb => sub {
                  function_not_found();
                },
                source_name => 'ae');

  $db->execute ('select * from foo', undef, cb => sub {
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;
  ok not $@;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_die_by_error

sub _execute_cb_error_die : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $line;
  my $warn;
  {
    local $SIG{__WARN__} = sub {
      $warn = $_[0];
    };

    $line = __LINE__ + 3;
    $db->execute ('select syntax error', undef,
                  cb => sub {
                    die "hoge";
                  },
                  source_name => 'ae');

    $db->execute ('select * from foo', undef, cb => sub {
      $cv->send;
    }, source_name => 'ae');

    $cv->recv;
  };

  ok not $@;
  is $warn, 'Died within handler: hoge at ' . __FILE__ . ' line ' . $line . ".\n";

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_die

sub _execute_cb_onerror_order : Test(1) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my @error;
  $db->onerror (sub {
    push @error, 'onerror';
  });

  my $cv = AnyEvent->condvar;
  $db->execute ('select syntax error', undef, cb => sub {
    push @error, 'cb';
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  eq_or_diff \@error, ['cb', 'onerror'];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_onerror_order

sub _execute_cb_result_table_name : Test(1) {
  my $db = new_db;
  my $cv = AE::cv;
  $db->execute
      ('create table foo (id int)', undef, table_name => 'hoge',
       cb => sub {
         my (undef, $result) = @_;
         is $result->table_name, 'hoge';
         $cv->send;
       });
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_result_table_name

sub _execute_cb_all_insert : Test(13) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  $db->execute ('insert into foo (id) values (3)', undef,
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_all_insert

sub _execute_cb_all_insert_each_cb : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked = 0;
  $db->execute ('insert into foo (id) values (3)', undef,
                each_cb => sub {
                  $invoked++;
                },
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  is $invoked, 0;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_all_insert_each_cb

sub _execute_cb_all_insert_each_as_row_cb : Test(14) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked = 0;
  $db->execute ('insert into foo (id) values (3)', undef,
                each_as_row_cb => sub {
                  $invoked++;
                },
                table_name => 'xab',
                cb => sub {
                  is $_[0], $db;
                  $result = $_[1];
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  is $invoked, 0;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  is $result->row_count, 1;
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _execute_cb_all_insert_each_as_row_cb

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
