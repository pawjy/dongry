package Dict::Context::EntryList;
use strict;
use warnings;

sub new ($%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub category_name {
  my $self = shift;
  if (@_) {
    $self->{category_name} = $_[0];
  }
  return $self->{category_name};
} # category_name

sub category_name_lang {
  my $self = shift;
  if (@_) {
    $self->{category_name_lang} = $_[0];
  }
  return $self->{category_name_lang};
} # category_name_lang

sub category {
  my $self = shift;
  require Dict::Category;
  return $self->{category}
      ||= Dict::Category->new_from_category_name_in_lang
          ($self->{category_name}, $self->{category_name_lang});
} # category

sub query {
  my $self = shift;
  my $cat = $self->category;
  return Dongry::Database->load ('dict')->query unless $cat->is_existing;

  require Dongry::Database;
  require Dict::Entry;
  my $desc = Dongry::Database->load ('dict')->table ('description');
  return $self->{query} ||= Dongry::Database->load ('dict')->query
      (table_name => 'entry',
       where => {
           category_id => $cat->category_id,
       },
       order => [qw(updated_on DESC)],
       item_list_filter => sub {
         my $list = $_[1]->map(sub { Dict::Entry->new_from_entry_row ($_) });
         
         $desc->fill_related_rows
             ($list, {'category_id' => 'category_id', 'entry_id' => 'entry_id'}
              => 'set_description_rows',
              multiple => 1);
         
         return $list;
       });
} # query

sub per_page { 3 }

sub page {
  my $self = shift;
  if (@_) {
    $self->{page} = $_[0];
  }
  return $self->{page} || 1;
} # page

sub items {
  my $self = shift;
  return $self->{items} ||= $self->query->find_all
      (offset => ($self->page - 1) * $self->per_page,
       limit => $self->per_page);
} # items

1;
