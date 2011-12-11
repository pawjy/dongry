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
  
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 0;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };
  }
} # _delete_nop

sub _delete_a_row_deleted : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted

sub _delete_two_row_deleted : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12), (12), (13)');
    
    my $result = $db->delete ('foo', {id => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 2;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->select
        ('foo', ['1 = 1'],
         source_name => 'master',
         order => [id => 1])->all->to_a,
             [{id => 13}];
  }
} # _delete_two_rows_deleted

sub _delete_a_row_deleted_utf8_flagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute (qq{insert into foo (id) values ("\x{5000}")});
    
    my $result = $db->delete ('foo', {id => "\x{5000}"});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_flagged_value

sub _delete_a_row_deleted_utf8_unflagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
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
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_unflagged_value

sub _delete_a_row_deleted_stupid_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute ('insert into foo (id) values ("a ` b);")');
    
    my $result = $db->delete ('foo', {id => "a ` b);"});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_stupid_value

sub _delete_a_row_deleted_utf8_flagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (`\x{5000}` blob)");
    $db->execute ("insert into foo (`\x{5000}`) values (12)");
    
    my $result = $db->delete ('foo', {"\x{5000}" => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_flagged_column

sub _delete_a_row_deleted_utf8_unflagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
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
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [];
  }
} # _delete_a_row_deleted_utf8_unflagged_column

sub _delete_a_row_deleted_stupid_column : Test(12) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (```ab(;` blob)");
    $db->execute ("insert into foo (```ab(;`) values (12)");
    
    dies_ok {
      my $result = $db->delete
          ('foo', {(encode 'utf-8', "`ab(;") => 12});
    };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [{(encode 'utf-8', "`ab(;") => 12}];

    next;

    my $result = $db->delete
        ('foo', {(encode 'utf-8', "`ab(;") => 12});
    isa_ok $result, 'Dongry::Database::Executed';
    is $result->row_count, 1;
    is $result->table_name, 'foo';
    my $invoked = 0;
    dies_ok { $result->$method (sub { $invoked++ }) };
    dies_ok { $result->all };
    dies_ok { $result->all_as_rows };
    dies_ok { $result->each (sub { $invoked++ }) };
    dies_ok { $result->each_as_row (sub { $invoked++ }) };
    is $invoked, 0;
    dies_ok { $result->first };
    dies_ok { $result->first_as_row };

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
  
  dies_ok {
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
  
  dies_ok {
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
  
  dies_ok {
    my $result = $db->delete
        ('foo', 'id > 23');
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
  
  dies_ok {
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
  
  dies_ok {
    my $result = $db->delete ('foo');
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _delete_where_no_arg

__PACKAGE__->runtests;

1;
