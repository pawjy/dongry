package Dict::Service::EditEntry;
use strict;
use warnings;
use Dict::Databases;

sub new {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub lang {
  if (@_ > 1) {
    $_[0]->{lang} = $_[1];
  }
  return $_[0]->{lang};
} # lang

sub category {
  return $_[0]->{category};
} # category

sub entry {
  return $_[0]->{entry};
} # entry

sub create_category_if_necessary {
  my ($self, %args) = @_;
  my $lang = $self->lang or die "No lang";

  require Dongry::Database;
  my $dictdb = Dongry::Database->load ('dict');
  
  my $transaction = $dictdb->transaction;
  my $cat_row = $dictdb->table ('category')->find
      ({'name_' . $lang => $args{category_name}},
       source_name => 'master',
       lock => 'update');
  unless ($cat_row) {
    $cat_row = $dictdb->table ('category')->create
        ({'name_' . $lang => $args{category_name}});
    $cat_row->{data}->{id} = $dictdb->last_insert_id;
  }
  $transaction->commit;

  require Dict::Category;
  $self->{category} = Dict::Category->new_from_row ($cat_row);
} # create_category_if_necessary

sub create_entry_if_necessary {
  my ($self, %args) = @_;
  my $lang = $self->lang or die "No lang";

  require Dongry::Database;
  my $dictdb = Dongry::Database->load ('dict');
  
  my $transaction = $dictdb->transaction;
  my $entry_row = $dictdb->table ('entry')->find
      ({category_id => $self->category->category_id,
        'title_' . $lang => $args{entry_title}},
       source_name => 'master',
       lock => 'update');
  unless ($entry_row) {
    my $entry_id = $dictdb->select
        ('entry',
         {category_id => $self->category->category_id},
         fields => \'MAX(entry_id) + 1 AS value',
         source_name => 'master',
         lock => 'update')->first->{value} || 1;
    $entry_row = $dictdb->table ('entry')->create
        ({category_id => $self->category->category_id,
          entry_id => $entry_id,
          'title_' . $lang => $args{entry_title}});
  }
  $transaction->commit;

  require Dict::Entry;
  $self->{entry} = Dict::Entry->new_from_entry_row ($entry_row);
} # create_entry_if_necessary

sub update_text {
  my ($self, %args) = @_;
  my $lang = $self->lang or die "No lang";

  require Dongry::Database;
  my $dictdb = Dongry::Database->load ('dict');

  my $transaction = $dictdb->transaction;
  my $desc_row = $dictdb->table ('description')->find
      ({category_id => $self->category->category_id,
        entry_id => $self->entry->entry_id,
        lang => $lang},
       source_name => 'master',
       lock => 'update');
  unless ($desc_row) {
    $desc_row = $dictdb->table ('description')->create
        ({category_id => $self->category->category_id,
          entry_id => $self->entry->entry_id,
          lang => $lang});
  }

  my $meta = $desc_row->get ('metadata');
  $meta->{keywords} = $args{keywords} || [];
  $meta->{updated}->{+time} = 1;
  $desc_row->update
      ({text => $args{entry_body_as_ref}, metadata => $meta});
  $transaction->commit;

  $self->entry->description_row_in_lang ($lang); # Load other langs
  $self->entry->{description_rows}->{$lang} = $desc_row;
} # update_text

1;
