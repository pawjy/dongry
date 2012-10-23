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
} # _execute_cb_first

sub _execute_cb_each : Test(14) {
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
  $result->each (sub { push @values, $_ });
  eq_or_diff \@values, [{id => 1}, {id => 2}];
  dies_here_ok { $result->all };
  dies_here_ok { $result->first };
  my $invoked;
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;
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
} # _execute_cb_all_as_rows

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
  like $result->error_text, qr[Can't connect|Unknown database|Access denied];
  is $result->error_sql, 'create table foo (id int)';
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
  like $result->error_text, qr{Can't connect|Unknown database|Access denied};
  is $result->error_sql, 'create table foo (id int)';

  $result = undef;
  $cv = AnyEvent->condvar;

  $db->execute ('hoge', undef, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
  }, source_name => 'ae');
  
  is $invoked, 2;
  like $result->error_text, qr{Can't connect|Unknown database|Access denied};
  is $result->error_sql, 'hoge';
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

  eval {
    $cv->recv;
  };
  like $@, qr{^hoge at }; ## file/line is meaningless
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

  eval {
    $cv->recv;
  };
  is $@, 'hoge at ' . __FILE__ . ' line ' . (__LINE__ - 7) . ".\n";
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

  eval {
    $cv->recv;
  };
  like $@,
      qr{function_not_found.* at \Q@{[__FILE__]} line @{[__LINE__ - 8]}.\E$};
} # _execute_cb_die_by_error

sub _execute_cb_error_die : Test(3) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $line;
  my $warn;
  {
    local $SIG{__WARN__} = sub {
      $warn = $_[0];
    };

    my $cv = AnyEvent->condvar;

    $line = __LINE__ + 3;
    $db->execute ('select syntax error', undef,
                  cb => sub {
                    die "hoge";
                  },
                  source_name => 'ae');

    eval {
      $cv->recv;
      1;
    } or do {
      ok 1;
    };
  };
  ok defined $@; ## Actual exception is hidden by AnyEvent::DBI.
  is $warn, 'hoge at ' . __FILE__ . ' line ' . $line . ".\n";
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
} # _execute_cb_result_table_name

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
