package test::Dongry::Database::delete;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

sub _delete_nop : Test(66) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int)');
  
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 0;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };
  }
} # _delete_nop

sub _delete_a_row_deleted : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted

sub _delete_two_row_deleted : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12), (12), (13)');
    
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 2;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->select
        ('foo', ['1 = 1'],
         source_name => 'master',
         order => [id => 1])->all->to_a,
             [{id => 13}];
  }
} # _delete_two_rows_deleted

sub _delete_a_row_deleted_utf8_flagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute (qq{insert into foo (id) values ("\x{5000}")});
    
    my $result = $db->delete ('foo', {id => "\x{5000}"});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_flagged_value

sub _delete_a_row_deleted_utf8_unflagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute (qq{insert into foo (id) values ("\x{6000}")});
    
    my $result = $db->delete
        ('foo', {id => encode 'utf-8', "\x{6000}"});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_unflagged_value

sub _delete_a_row_deleted_stupid_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute ('insert into foo (id) values ("a ` b);")');
    
    my $result = $db->delete ('foo', {id => "a ` b);"});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_stupid_value

sub _delete_a_row_deleted_utf8_flagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (`\x{5000}` blob)");
    $db->execute ("insert into foo (`\x{5000}`) values (12)");
    
    my $result = $db->delete ('foo', {"\x{5000}" => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_flagged_column

sub _delete_a_row_deleted_utf8_unflagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (`\x{5000}` blob)");
    $db->execute ("insert into foo (`\x{5000}`) values (12)");
    
    my $result = $db->delete
        ('foo', {(encode 'utf-8', "\x{5000}") => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_unflagged_column

sub _delete_a_row_deleted_stupid_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_row first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (```ab(;` blob)");
    $db->execute ("insert into foo (```ab(;`) values (12)");

    my $result = $db->delete
        ('foo', {(encode 'utf-8', "`ab(;") => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    ng $result->table_name;
    my $invoked = 0;
    dies_here_ok { $result->$method (sub { $invoked++ }) };
    dies_here_ok { $result->all };
    dies_here_ok { $result->all_as_rows };
    dies_here_ok { $result->each (sub { $invoked++ }) };
    dies_here_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_here_ok { $result->first };
    dies_here_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_stupid_column

sub _delete_table_stupid : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `2f``b` (id int, v1 blob)");
  $db->execute ("insert into `2f``b` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `2f``b` (id, v1) values (25, 'xycde')");
  
  my $result = $db->delete
      ('2f`b', ['id = 25']);

  eq_or_diff $db->execute
      ('select * from `2f``b`', undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}];
} # _delete_table_stupid

sub _delete_table_utf8_flagged : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `\x{1000}` (id int, v1 blob)");
  $db->execute ("insert into `\x{1000}` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `\x{1000}` (id, v1) values (25, 'xycde')");
  
  my $result = $db->delete
      ("\x{1000}", ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `\x{1000}`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}];
} # _delete_table_utf8_flagged

sub _delete_table_utf8_unflagged : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `\x{1000}` (id int, v1 blob)");
  $db->execute ("insert into `\x{1000}` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `\x{1000}` (id, v1) values (25, 'xycde')");
  
  my $result = $db->delete
      ((encode 'utf-8', "\x{1000}"), ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `\x{1000}`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}];
} # _delete_table_utf8_unflagged

sub _delete_where_sqla : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->delete
      ('foo', {id => {'>', 20}});
  is $result->row_count, 1;

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}];
} # _delete_where_sqla

sub _delete_where_sqlp : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->delete
      ('foo', ['id > ?', id => 23]);
  is $result->row_count, 1;

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}];
} # _delete_where_sqlp

sub _delete_where_bad_column : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_here_ok {
    my $result = $db->delete
        ('foo', ['mid > 23']);
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_bad_column

sub _delete_where_bad_sql : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_here_ok {
    my $result = $db->delete
        ('foo', ['id id id ']);
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_bad_sql

sub _delete_where_bad_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_here_ok {
    my $result = $db->delete ('foo', 'id > 23');
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_bad_arg

sub _delete_where_empty_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_here_ok {
    my $result = $db->delete ('foo', {});
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_empty_arg

sub _delete_where_no_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_here_ok {
    my $result = $db->delete ('foo');
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_no_arg

sub _delete_source_name_implicit : Test(4) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';
  
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ("create table foo (id int, v1 blob)");
  $db2->execute ("create table foo (id int, v1 blob)");
  $db3->execute ("create table foo (id int, v1 blob)");

  $db1->execute ("insert into foo (id, v1) values (11, 'ab cde')");
  $db2->execute ("insert into foo (id, v1) values (21, 'ab cde')");
  $db3->execute ("insert into foo (id, v1) values (31, 'ab cde')");

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  my $result = $db->delete ('foo', {v1 => 'ab cde'});
  is $result->row_count, 1;

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 21}];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _delete_source_name_implicit

sub _delete_source_name_implicit_not_writable : Test(4) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';
  
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ("create table foo (id int, v1 blob)");
  $db2->execute ("create table foo (id int, v1 blob)");
  $db3->execute ("create table foo (id int, v1 blob)");

  $db1->execute ("insert into foo (id, v1) values (11, 'ab cde')");
  $db2->execute ("insert into foo (id, v1) values (21, 'ab cde')");
  $db3->execute ("insert into foo (id, v1) values (31, 'ab cde')");

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 0},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});

  dies_here_ok {
    my $result = $db->delete ('foo', {v1 => 'ab cde'});
  };

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 11}];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 21}];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _delete_source_name_implicit_not_writable

sub _delete_source_name_default : Test(4) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';
  
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ("create table foo (id int, v1 blob)");
  $db2->execute ("create table foo (id int, v1 blob)");
  $db3->execute ("create table foo (id int, v1 blob)");

  $db1->execute ("insert into foo (id, v1) values (11, 'ab cde')");
  $db2->execute ("insert into foo (id, v1) values (21, 'ab cde')");
  $db3->execute ("insert into foo (id, v1) values (31, 'ab cde')");

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 1},
                   heavy => {dsn => $dsn3, writable => 1}});
  my $result = $db->delete ('foo', {v1 => 'ab cde'},
                            source_name => 'default');
  is $result->row_count, 1;

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 11}];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _delete_source_name_default

sub _delete_source_name_default_not_writable : Test(4) {
  reset_db_set;

  my $dsn1 = test_dsn 'test1';
  my $dsn2 = test_dsn 'test2';
  my $dsn3 = test_dsn 'test3';
  
  my $db1 = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn2, writable => 1}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn3, writable => 1}});

  $db1->execute ("create table foo (id int, v1 blob)");
  $db2->execute ("create table foo (id int, v1 blob)");
  $db3->execute ("create table foo (id int, v1 blob)");

  $db1->execute ("insert into foo (id, v1) values (11, 'ab cde')");
  $db2->execute ("insert into foo (id, v1) values (21, 'ab cde')");
  $db3->execute ("insert into foo (id, v1) values (31, 'ab cde')");

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn1, writable => 1},
                   default => {dsn => $dsn2, writable => 0},
                   heavy => {dsn => $dsn3, writable => 1}});
  dies_here_ok {
    my $result = $db->delete ('foo', {v1 => 'ab cde'},
                              source_name => 'default');
  };

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 11}];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 21}];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _delete_source_name_default_not_writable

sub _delete_offset_none : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12}, offset => undef);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [];
} # _delete_offset_none

sub _delete_offset_0 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_here_ok {
    my $result = $db->delete
        ('foo', {id => 12},
         order => [v1 => 1], offset => 0);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _delete_offset_0

sub _delete_offset_1 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_here_ok {
    my $result = $db->delete
        ('foo', {id => 12},
         order => [v1 => 1], offset => 1);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _delete_offset_1

sub _delete_limit_none : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => 1], limit => undef);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [];
} # _delete_limit_none

sub _delete_limit_0 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => 1], limit => 0);
  is $result->row_count, 1;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _delete_limit_0

sub _delete_limit_1 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => 1], limit => 1);
  is $result->row_count, 1;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _delete_limit_1

sub _delete_limit_2 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => 1], limit => 2);
  is $result->row_count, 2;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 3, v2 => undef}];
} # _delete_limit_2

sub _delete_limit_large : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => 1], limit => 200000);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [];
} # _delete_limit_large

sub _delete_offset_limit : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_here_ok {
    my $result = $db->delete
        ('foo', {id => 12},
         order => [v1 => 1], offset => 1, limit => 2);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _delete_offset_limit

sub _delete_order_desc_limit : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->delete
      ('foo', {id => 12},
       order => [v1 => -1], limit => 2);
  is $result->row_count, 2;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef}];
} # _delete_order_desc_limit

sub _delete_cb : Test(10) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (12), (423)');
  
  my $result;
  $db->delete ('foo', {id => 12}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 1;
  ng $result->table_name;
  dies_here_ok { $result->each (sub { }) };

  eq_or_diff $db->select ('foo', {id => {-not => undef}},
                          order => [id => 1])->all->to_a,
      [{id => 423}];
} # _delete_cb

sub _delete_cb_return : Test(12) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (12), (423)');
  
  my $result;
  my $result2 = $db->delete
      ('foo', {id => 12}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
  });

  is $result2, $result;
  is $result2->row_count, $result->row_count;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 1;
  ng $result->table_name;
  dies_here_ok { $result->each (sub { }) };

  eq_or_diff $db->select ('foo', {id => {-not => undef}},
                          order => [id => 1])->all->to_a,
      [{id => 423}];
} # _delete_cb_return

sub _delete_cb_exception : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (12), (423)');
  
  my $result;
  eval {
    $db->delete ('foo', {id => 12}, cb => sub {
      die "ab cd";
    });
    ng 1;
  };
  is $@, 'ab cd at ' . __FILE__ . ' line ' . (__LINE__ - 4) . ".\n";

  eq_or_diff $db->select ('foo', {id => {-not => undef}},
                          order => [id => 1])->all->to_a,
      [{id => 423}];
} # _delete_cb_exception

sub _delete_cb_error : Test(2) {
  my $db = new_db;
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (12), (423)');
  
  my $result;
  my $invoked;
  eval {
    $db->delete ('bar', {id => 12}, cb => sub {
      $invoked++;
    });
    ng 1;
  };
  like $@, qr{bar};

  eq_or_diff $db->select ('foo', {id => {-not => undef}},
                          order => [id => 1])->all->to_a,
      [{id => 12}, {id => 423}];
} # _delete_cb_error

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
