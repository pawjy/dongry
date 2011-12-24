package Dict::Context::CategoryList;
use strict;
use warnings;

sub new ($%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub query {
  my $self = shift;
  require Dongry::Database;
  require Dict::Category;
  return $self->{query} ||= Dongry::Database->load ('dict')->query
      (table_name => 'category',
       where => {1 => 1},
       order => [qw(updated_on DESC)],
       item_list_filter => sub {
         return $_[1]->map (sub { Dict::Category->new_from_row ($_) });
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
  return $self->{items} ||= $self->query->search
      (offset => ($self->page - 1) * $self->per_page,
       limit => $self->per_page);
} # items

1;
