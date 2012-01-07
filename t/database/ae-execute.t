package test::Dongry::Database::anyevent::execute;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _execute_cb_all : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
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
  ng $error;
} # _execute_cb_all

sub _execute_cb_first : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
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
  ng $error;
} # _execute_cb_first

sub _execute_cb_each : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
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
  ng $error;
} # _execute_cb_each

sub _execute_cb_all_as_rows : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
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
  ng $error;
} # _execute_cb_all_as_rows

sub _execute_cb_first_as_row : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
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
  ng $error;
} # _execute_cb_first_as_row

sub _execute_cb_each_as_row : Test(12) {
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
                onerror => sub {
                  $error++;
                },
                source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  is $result->row_count, 2;
  my $invoked;
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  isa_list_ok $result->all;
  dies_here_ok { $result->first };
  dies_here_ok { $result->each (sub { $invoked++ }) };
  dies_here_ok { $result->all_as_rows };
  dies_here_ok { $result->first_as_row };
  dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
  ng $invoked;
  ng $error;
} # _execute_cb_each_as_row

sub _execute_syntax_error : Test(5) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $result;
  my $success;
  my $error;
  my $error_text;
  my $error_sql;
  $db->execute ('select * from', undef,
                cb => sub {
                  $success++;
                  $cv->send;
                },
                onerror => sub {
                  my ($self, %args) = @_;
                  is $self, $db;
                  $error_text = $args{text};
                  $error_sql = $args{sql};
                  $error++;
                  $cv->send;
                },
                source_name => 'ae');

  $cv->recv;

  ng $success;
  is $error, 1;
  like $error_text, qr{syntax};
  is $error_sql, 'select * from';
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
                onerror => sub {
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

sub _execute_syntax_error_followed : Test(3) {
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

  my $success;
  my $error;
  $cv->begin;
  $db->execute ('select * from foo', undef, source_name => 'ae',
                cb => sub { 
                  $success++;
                  $cv->end;
                }, onerror => sub {
                  is $_[0], $db;
                  $error++;
                  $cv->end;
                });

  $cv->end;
  $cv->recv;

  ng $success;
  is $error, 1;
} # _execute_syntax_error_followed

sub _execute_syntax_error_followed_2 : Test(3) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2)');
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;

  my $success;
  my $error;
  $db->execute ('select * from', undef, source_name => 'ae',
                cb => sub { $cv->send }, onerror => sub {
                  $db->execute ('select * from foo', undef,
                                source_name => 'ae',
                                cb => sub {
                                  $success++;
                                  $cv->send;
                                }, onerror => sub {
                                  is $_[0], $db;
                                  $error++;
                                  $cv->send;
                                });
                });

  $cv->recv;

  ng $success;
  is $error, 1;
} # _execute_syntax_error_followed_2

sub _execute_connection_error : Test(5) {
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'dsi:mysql:foo:bar', anyevent => 1,
                          writable => 1}});
  
  my $cv = AnyEvent->condvar;
  
  my $success;
  my $error;
  my $error_info;
  $db->execute ('create table foo (id int)', undef,
                source_name => 'ae',
                cb => sub {
                  $success++;
                  $cv->send;
                },
                onerror => sub {
                  my ($self, %args) = @_;
                  is $self, $db;
                  $error_info = \%args;
                  $error++;
                  $cv->send;
                });

  $cv->recv;
  ng $success;
  is $error, 1;
  like $error_info->{text}, qr{Can't connect};
  is $error_info->{sql}, 'create table foo (id int)';
} # _execute_connection_error

sub _execute_connection_error_2 : Test(10) {
  my $db = Dongry::Database->new
      (sources => {ae => {dsn => 'dsi:mysql:foo:bar', anyevent => 1,
                          writable => 1}});
  
  my $cv = AnyEvent->condvar;
  
  my $success;
  my $error;
  my $error_info;
  $db->execute ('create table foo (id int)', undef,
                source_name => 'ae',
                cb => sub {
                  $success++;
                  $cv->send;
                },
                onerror => sub {
                  my ($self, %args) = @_;
                  is $self, $db;
                  $error_info = \%args;
                  $error++;
                  $cv->send;
                });

  $cv->recv;

  isa_ok $db->{dbhs}->{ae}, 'Dongry::Database::BrokenConnection';
  ng $success;
  is $error, 1;
  like $error_info->{text}, qr{Can't connect};
  is $error_info->{sql}, 'create table foo (id int)';

  $cv = AnyEvent->condvar;

  $db->execute ('hoge', undef, cb => sub {
    $success++;
  }, onerror => sub {
    my ($self, %args) = @_;
    is $self, $db;
    $error_info = \%args;
    $error++;
  }, source_name => 'ae');
  
  is $error, 2;
  like $error_info->{text}, qr{Can't connect};
  is $error_info->{sql}, 'hoge';
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
                             onerror => sub { $cv->send },
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

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
