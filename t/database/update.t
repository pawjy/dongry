package test::Dongry::Database::update;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

sub _update_nop : Test(66) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ('create table foo (id int)');
  
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    my $result = $db->update ('foo', {id => 23}, {id => 12});
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
} # _update_nop

sub _update_a_row_updated : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->update ('foo', {id => 23}, {id => 12});
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
        [{id => 23}];
  }
} # _update_a_row_updated

sub _update_two_row_updated : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id int)');
    $db->execute ('insert into foo (id) values (12), (12), (13)');
    
    my $result = $db->update ('foo', {id => 23}, {id => 12});
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
             [{id => 13}, {id => 23}, {id => 23}];
  }
} # _update_two_rows_updated

sub _update_a_row_updated_utf8_flagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->update ('foo', {id => "\x{5000}"}, {id => 12});
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
        [{id => encode 'utf-8', "\x{5000}"}];
  }
} # _update_a_row_updated_utf8_flagged_value

sub _update_a_row_updated_utf8_unflagged_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->update
        ('foo', {id => encode 'utf-8', "\x{5000}"}, {id => 12});
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
        [{id => encode 'utf-8', "\x{5000}"}];
  }
} # _update_a_row_updated_utf8_unflagged_value

sub _update_a_row_updated_stupid_value : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ('create table foo (id blob)');
    $db->execute ('insert into foo (id) values (12)');
    
    my $result = $db->update ('foo', {id => "a ` b);"}, {id => 12});
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
        [{id => encode 'utf-8', "a ` b);"}];
  }
} # _update_a_row_updated_stupid_value

sub _update_a_row_updated_utf8_flagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (`\x{5000}` blob)");
    $db->execute ("insert into foo (`\x{5000}`) values (12)");
    
    my $result = $db->update ('foo', {"\x{5000}" => 23}, {"\x{5000}" => 12});
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
        [{(encode 'utf-8', "\x{5000}") => 23}];
  }
} # _update_a_row_updated_utf8_flagged_column

sub _update_a_row_updated_utf8_unflagged_column : Test(72) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (`\x{5000}` blob)");
    $db->execute ("insert into foo (`\x{5000}`) values (12)");
    
    my $result = $db->update
        ('foo', {(encode 'utf-8', "\x{5000}") => 23},
         {(encode 'utf-8', "\x{5000}") => 12});
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
        [{(encode 'utf-8', "\x{5000}") => 23}];
  }
} # _update_a_row_updated_utf8_unflagged_column

sub _update_a_row_updated_stupid_column : Test(12) {
  for my $method (qw(all all_as_rows each each_as_rows first first_as_row)) {
    reset_db_set;
    my $dsn = test_dsn 'test1';
    my $db = Dongry::Database->new
        (sources => {master => {dsn => $dsn, writable => 1}});
    $db->execute ("create table foo (```ab(;` blob)");
    $db->execute ("insert into foo (```ab(;`) values (12)");
    
    dies_ok {
      my $result = $db->update
          ('foo', {(encode 'utf-8', "`ab(;") => 23},
           {(encode 'utf-8', "`ab(;") => 12});
    };

    eq_or_diff $db->execute
        ('select * from foo', undef, source_name => 'master')->all->to_a,
        [{(encode 'utf-8', "`ab(;") => 12}];

    next;

    my $result = $db->update
        ('foo', {(encode 'utf-8', "`ab(;") => 23},
         {(encode 'utf-8', "`ab(;") => 12});
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
        [{(encode 'utf-8', "`ab(;") => 23}];
  }
} # _update_a_row_updated_stupid_column

sub _update_table_stupid : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `2f``b` (id int, v1 blob)");
  $db->execute ("insert into `2f``b` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `2f``b` (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ('2f`b', {id => 100}, ['id = 25']);

  eq_or_diff $db->execute
      ('select * from `2f``b`', undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 100, v1 => 'xycde'}];
} # _update_table_stupid

sub _update_table_utf8_flagged : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `\x{1000}` (id int, v1 blob)");
  $db->execute ("insert into `\x{1000}` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `\x{1000}` (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ("\x{1000}", {id => 100}, ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `\x{1000}`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 100, v1 => 'xycde'}];
} # _update_table_utf8_flagged

sub _update_table_utf8_unflagged : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table `\x{1000}` (id int, v1 blob)");
  $db->execute ("insert into `\x{1000}` (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into `\x{1000}` (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ((encode 'utf-8', "\x{1000}"), {id => 100}, ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `\x{1000}`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 100, v1 => 'xycde'}];
} # _update_table_utf8_unflagged

sub _update_values_bad_column : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {mid => 100}, ['id = 25']);
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_values_bad_column

sub _update_values_latin1_value : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ('foo', {v1 => "\x{c0}\x{91}\x{fe}"}, ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `foo`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => "\xC0\x91\xFE"}];
} # _update_values_latin1_value

sub _update_values_multiple : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ('foo', {v1 => "\x{c0}\x{91}\x{fe}",
               v2 => "\x{6001}\x{1201}\x{FF}"}, ['id = 25']);

  eq_or_diff $db->execute
      ("select * from `foo`", undef,
       source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde', v2 => undef},
           {id => 25, v1 => "\xC0\x91\xFE",
            v2 => (encode 'utf-8', "\x{6001}\x{1201}\x{00FF}")}];
} # _update_values_multiple

sub _update_where_sqla : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ('foo', {id => 100}, {id => {'>', 20}});
  is $result->row_count, 1;

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 100, v1 => 'xycde'}];
} # _update_where_sqla

sub _update_where_sqlp : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  my $result = $db->update
      ('foo', {id => 100}, ['id > ?', id => 23]);
  is $result->row_count, 1;

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 100, v1 => 'xycde'}];
} # _update_where_sqlp

sub _update_where_bad_column : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {id => 100}, ['mid > 23']);
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_where_bad_column

sub _update_where_bad_sql : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {id => 100}, ['id id id ']);
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_where_bad_sql

sub _update_where_bad_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {id => 100}, 'id > 23');
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_where_bad_arg

sub _update_where_empty_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {id => 100}, {});
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_where_empty_arg

sub _update_where_no_arg : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 'ab cde')");
  $db->execute ("insert into foo (id, v1) values (25, 'xycde')");
  
  dies_ok {
    my $result = $db->update
        ('foo', {id => 100});
  };

  eq_or_diff $db->execute
      ('select * from foo', undef, source_name => 'master', order => {id => 1})
      ->all->to_a,
          [{id => 12, v1 => 'ab cde'}, {id => 25, v1 => 'xycde'}];
} # _update_where_no_arg

sub _update_source_name_implicit : Test(4) {
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
  my $result = $db->update ('foo', {id => 100}, {v1 => 'ab cde'});
  is $result->row_count, 1;

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 100}];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 21}];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _update_source_name_implicit

sub _update_source_name_implicit_not_writable : Test(4) {
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

  dies_ok {
    my $result = $db->update ('foo', {id => 100}, {v1 => 'ab cde'});
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
} # _update_source_name_implicit_not_writable

sub _update_source_name_default : Test(4) {
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
  my $result = $db->update ('foo', {id => 100}, {v1 => 'ab cde'},
                            source_name => 'default');
  is $result->row_count, 1;

  eq_or_diff $db1->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 11}];
  eq_or_diff $db2->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 100}];
  eq_or_diff $db3->execute
      ('select id from foo', undef, source_name => 'master')->all->to_a,
      [{id => 31}];
} # _update_source_name_default

sub _update_source_name_default_not_writable : Test(4) {
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
  dies_ok {
    my $result = $db->update ('foo', {id => 100}, {v1 => 'ab cde'},
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
} # _update_source_name_default_not_writable

sub _update_offset_none : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12}, offset => undef);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => 'changed'},
       {id => 12, v1 => 3, v2 => 'changed'}];
} # _update_offset_none

sub _update_offset_0 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_ok {
    my $result = $db->update
        ('foo', {v2 => 'changed'}, {id => 12},
         order => [v1 => 1], offset => 0);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_offset_0

sub _update_offset_1 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_ok {
    my $result = $db->update
        ('foo', {v2 => 'changed'}, {id => 12},
         order => [v1 => 1], offset => 1);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_offset_1

sub _update_limit_none : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => 1], limit => undef);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => 'changed'},
       {id => 12, v1 => 3, v2 => 'changed'}];
} # _update_limit_none

sub _update_limit_0 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => 1], limit => 0);
  is $result->row_count, 1;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_limit_0

sub _update_limit_1 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => 1], limit => 1);
  is $result->row_count, 1;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_limit_1

sub _update_limit_2 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => 1], limit => 2);
  is $result->row_count, 2;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => 'changed'},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_limit_2

sub _update_limit_large : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => 1], limit => 200000);
  is $result->row_count, 3;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => 'changed'},
       {id => 12, v1 => 2, v2 => 'changed'},
       {id => 12, v1 => 3, v2 => 'changed'}];
} # _update_limit_large

sub _update_offset_limit : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  dies_ok {
    my $result = $db->update
        ('foo', {v2 => 'changed'}, {id => 12},
         order => [v1 => 1], offset => 1, limit => 2);
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => undef},
       {id => 12, v1 => 3, v2 => undef}];
} # _update_offset_limit

sub _update_order_desc_limit : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (12, 2)");
  $db->execute ("insert into foo (id, v1) values (12, 3)");

  my $result = $db->update
      ('foo', {v2 => 'changed'}, {id => 12},
       order => [v1 => -1], limit => 2);
  is $result->row_count, 2;

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 12, v1 => 2, v2 => 'changed'},
       {id => 12, v1 => 3, v2 => 'changed'}];
} # _update_order_desc_limit

sub _update_duplicate_error : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int unique key, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (22, 2)");
  $db->execute ("insert into foo (id, v1) values (32, 3)");

  dies_ok {
    my $result = $db->update
        ('foo', {id => 22}, {id => 12});
  };

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 22, v1 => 2, v2 => undef},
       {id => 32, v1 => 3, v2 => undef}];
} # _update_duplicate_error

sub _update_duplicate_ignore : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db->execute ("create table foo (id int unique key, v1 int, v2 blob)");
  $db->execute ("insert into foo (id, v1) values (12, 1)");
  $db->execute ("insert into foo (id, v1) values (22, 2)");
  $db->execute ("insert into foo (id, v1) values (32, 3)");

  my $result = $db->update
      ('foo', {id => 22}, {id => 12}, duplicate => 'ignore');
  is $result->row_count, 1; # !

  eq_or_diff $db->execute ('select * from foo order by id asc, v1 asc', undef,
                           source_name => 'master')->all->to_a,
      [{id => 12, v1 => 1, v2 => undef},
       {id => 22, v1 => 2, v2 => undef},
       {id => 32, v1 => 3, v2 => undef}];
} # _update_duplicate_ignore

__PACKAGE__->runtests;

1;
