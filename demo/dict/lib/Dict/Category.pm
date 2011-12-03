package Dict::Category;
use strict;
use warnings;
use Dict::Databases;

sub new_from_category_id ($$) {
  return bless {category_id => $_[1]}, $_[0];
} # new_from_category_id

sub new_from_category_name_in_lang {
  return bless {category_name => $_[1], category_name_lang => $_[2]}, $_[0];
} # new_from_category_name_in_lang

sub new_from_row ($$) {
  return bless {category_id => $_[1]->get ('id'), row => $_[1]}, $_[0];
} # new_from_row

sub category_id ($) {
  return $_[0]->{category_id} //= do {
    my $row = $_[0]->row;
    if ($row) {
      $row->get ('id');
    } else {
      undef;
    }
  };
} # category_id

sub is_existing {
  return !!$_[0]->row;
} # is_existing

sub row ($) {
  require Dongry::Database;
  return $_[0]->{row} //= do {
    if ($_[0]->{category_id}) {
      Dongry::Database->load ('dict')
          ->table ('category')->find ({id => $_[0]->{category_id}});
    } else {
      Dongry::Database->load ('dict')
          ->table ('category')->find ({'name_' . $_[0]->{category_name_lang} => $_[0]->{category_name}});
    }
  }; # or undef
} # row

sub name_in_lang ($$) {
  return $_[0]->row->get ('name_' . $_[1]);
} # name_in_lang

sub updated_on ($) {
  return $_[0]->row->get ('created_on');
} # updated_on

1;
