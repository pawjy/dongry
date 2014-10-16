package test::Dongry::Table::anyevent::table;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use Test::MoreMore::Mock;
use base qw(Test::Class);
use AnyEvent;
use Dongry::Type::Time;

sub _insert_cb : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  eq_or_diff $result->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb

sub _insert_cb_exception : Test(2) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    die "abc";
  });

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;
  ok not $@;

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_exception

sub _insert_cb_error : Test(8) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->insert
      ([{id => 123, value2 => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a, [];

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{value2};
  ok $result->error_sql;
  dies_here_ok { $result->row_count };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_error

sub _insert_cb_exception_error : Test(3) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $warn = '';
  local $SIG{__WARN__} = sub { $warn .= $_[0] };

  my $cv = AnyEvent->condvar;

  $db->table ('foo')->insert
      ([{id => 123, value2 => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    die "abc";
  });

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;
  ok not $@;
  like $warn, qr{Died within handler: abc at};

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a, [];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_exception_error

sub _insert_cb_return : Test(7) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result = $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  dies_here_ok { $result->all };

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_return

sub _find_cb : Test(11) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  $db->table ('foo')->find ({id => {-gt => 4}},
                            order => [id => 1],
                            source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  isa_ok $value, 'Dongry::Table::Row';
  is $value->table_name, 'foo';
  eq_or_diff $value->{data}, {id => 12, value => '2012-01-01 00:12:12'};

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_cb

sub _find_cb_not_found : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  $db->table ('foo')->find ({id => {-gt => 400}},
                            order => [id => 1],
                            source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  is $value, undef;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_cb_not_found

sub _find_all_cb : Test(15) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  $db->table ('foo')->find_all ({id => {-gt => 4}},
                                order => [id => 1],
                                source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  isa_list_n_ok $value, 2;
  isa_ok $value->[0], 'Dongry::Table::Row';
  is $value->[0]->table_name, 'foo';
  eq_or_diff $value->[0]->{data}, {id => 12, value => '2012-01-01 00:12:12'};
  isa_ok $value->[1], 'Dongry::Table::Row';
  is $value->[1]->table_name, 'foo';
  eq_or_diff $value->[1]->{data}, {id => 21, value => '1991-02-12 12:12:01'};

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_all_cb

sub _find_cb_return : Test(5) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  my $return = $db->table ('foo')->find ({id => {-gt => 4}},
                                         order => [id => 1],
                                         source_name => 'ae', cb => sub {
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  });
  isa_ok $return, 'Dongry::Database::Executed';

  $cv->recv;
  is $invoked, 1;
  is $result->row_count, 1;
  dies_here_ok { $result->all };
  isa_ok $value, 'Dongry::Table::Row';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_cb_return

sub _find_cb_return_not_found : Test(5) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  my $return = $db->table ('foo')->find ({id => {-gt => 400}},
                                         order => [id => 1],
                                         source_name => 'ae', cb => sub {
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  });
  isa_ok $return, 'Dongry::Database::Executed';

  $cv->recv;
  is $invoked, 1;
  is $result->row_count, 0;
  dies_here_ok { $result->all };
  is $value, undef;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_cb_return_not_found

sub _find_all_cb_return : Test(7) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  my $return = $db->table ('foo')->find_all ({id => {-gt => 4}},
                                             order => [id => 1],
                                             source_name => 'ae', cb => sub {
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  });
  isa_ok $return, 'Dongry::Database::Executed';

  $cv->recv;
  is $invoked, 1;
  is $result->row_count, 2;
  dies_here_ok { $result->all };
  isa_list_n_ok $value, 2;
  isa_ok $value->[0], 'Dongry::Table::Row';
  isa_ok $value->[1], 'Dongry::Table::Row';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_all_cb_return

sub _find_cb_error : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  $db->table ('foo')->find ({id2 => 4},
                            order => [id => 1],
                            source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{id2};
  like $result->error_sql, qr{id2};
  ng $result->table_name;
  dies_here_ok { $result->all };
  is $value, undef;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_cb_error

sub _find_all_cb_error : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  $db->table ('foo')->find_all ({id2 => 4},
                                order => [id => 1],
                                source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{id2};
  like $result->error_sql, qr{id2};
  ng $result->table_name;
  dies_here_ok { $result->all };
  is $value, undef;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _find_all_cb_error

sub _fill_related_rows_cb : Test(5) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema, ae => 1;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _fill_related_rows_cb

sub _fill_related_rows_cb_error : Test(6) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema, ae => 1;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  my $cv = AnyEvent->condvar;

  my $result;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'notid'} => 'related_row', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{notid};
  ng $mock1->related_row;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _fill_related_rows_cb_error

sub _fill_related_rows_cb_none_error : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema, ae => 1;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  my $cv = AnyEvent->condvar;

  my $result0;
  my $result;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'notid'} => 'related_row',
       source_name => 'ae', cb => sub { $result0 = $_[1] });

  $table->find ({id => 124}, cb => sub { $result = $_[1]; $cv->send },
                source_name => 'ae');

  $cv->recv;

  ok $result0->is_error;
  ok $result->is_success;
  ng $mock1->related_row;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _fill_related_rows_cb_none_error

sub _fill_related_rows_cb_exception : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema, ae => 1;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  my $cv = AnyEvent->condvar;

  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row', cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;

  ok not $@;
  ok $mock1->related_row;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _fill_related_rows_cb_exception

sub _fill_related_rows_cb_exception_error : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema, ae => 1;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  my $warn = '';
  local $SIG{__WARN__} = sub { $warn .= $_[0] };

  my $cv = AnyEvent->condvar;

  $table->fill_related_rows
      ([$mock1] => {related_id => 'notid'} => 'related_row', cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;
  
  ok not $@;
  like $warn, qr<Died within handler: abc at >;
  ng $mock1->related_row;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _fill_related_rows_cb_exception_error

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
