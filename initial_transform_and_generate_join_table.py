import pandas as pd

def main():
    """
    Contains logic for dataset transformations to be done before upload to an RDBMS.

    Logic to both perform minor transformations on the users dataset to remove
    some decimal places from the id fields before uploading to a relational db,
    and to generate a "join" table to allow us to easily join the data between
    the universities data set and the dataset from aamc.org (which I call the
    "enrollment" dataset). 
    """

    # Loading datasets into Python/Pandas.
    users = pd.read_csv('1_source_files/users.csv')
    universities = pd.read_csv('1_source_files/universities.csv')
    enrollment = pd.read_csv('2_modified_files/2023_FACTS_Table_B_1_point_2_MODIFIED.csv')

    # Creating a primary key field for the Enrollment dataset. Adding 1 to the
    # index to ensure it matches with the index created in the postgres logic.
    enrollment["id"] = enrollment.index + 1

    # Removing the single decimal place after the id fields in the Users dataset to make them integers.
    users['id'] = pd.to_numeric(users['id'], downcast='integer')
    users['university_id'] = pd.to_numeric(users['university_id'], downcast='integer')

    # Writing the modified users table to a CSV, to be uploaded later into the postgres db.
    users.to_csv('2_modified_files/users_modified.csv', index=False)

    # Defining universities with a more descriptive name for our purposes, then
    # clearing the original from memory.
    universities_filtered = universities
    del(universities)

    # Filtering the universities datasets down to reduce records compared later.
    # First, removing non-U.S. universities (since the aamc dataset only includes U.S. universities).
    universities_filtered = universities_filtered.query("country == 'United States of America'")
    
    # Then, removing all universities that don't have "med" in their name.
    # (Assuming this to be a good way of ensuring we are selecting mostly
    # medical schools to filter.)
    universities_filtered['name'] = universities_filtered['name'].str.lower()
    universities_filtered = universities_filtered[universities_filtered['name'].str.contains('med')]

    # Removing all universities that do not have associated users in our users dataset using an inner join.
    universities_filtered = universities_filtered.merge(users[['university_id']], left_on = 'id', right_on = 'university_id', how='inner')

    # Last, dropping duplicates.
    universities_filtered = universities_filtered.drop_duplicates()

    # Clear the users df from memory.
    del(users)

    # To compare values, I am comparing individual words between the
    # universities dataset and the enrollment dataset. To facilitate this, I'm
    # splitting the school name for each dataset into a list of their
    # associated words. (Here, we are doing it for the universities dataset.)
    universities_filtered['name_split'] = universities_filtered['name'].str.split()

    # Then, I'm ordering that dataset to start with the longest length lists
    # first (meaning, the names with the most words). The intent is to start by
    # strongly matching the most complex school names first. That way, when we
    # get to the simpler school names (i.e. "Alabama", "Arkansas"), there are
    # fewer schools that can be falsely attributed because they happen to have
    # that single word in their name.
    universities_filtered = universities_filtered.sort_values('name_split', key=lambda s: s.str.len(), ascending=False)
    
    # Like universities_filtered above: Defining enrollment with a more
    # descriptive name for our purposes, then clearing the original from
    # memory.
    enrollment_filtered = enrollment
    del(enrollment)

    # As with the universities dataset above, I'm creating a list of the
    # individual words of their names. I also added string replace logic to
    # replace the dashes with a space, to ensure words connected by dashes are
    # still counted as separate words. (Also ordering the dataset by longest
    # name for the same reason as above.)
    enrollment_filtered['Medical_School'] = enrollment_filtered['Medical_School'].str.lower()
    enrollment_filtered['Medical_School_Split'] = enrollment_filtered['Medical_School'].str.replace('-', ' ')
    enrollment_filtered['Medical_School_Split'] = enrollment_filtered['Medical_School_Split'].str.split()
    enrollment_filtered = enrollment_filtered.sort_values('Medical_School_Split', key=lambda s: s.str.len(), ascending=False)
    
    # Creating the dataset to hold the join table.
    uni_enroll_matched = pd.DataFrame(columns=['uni_id','uni_name','enroll_id','enroll_name'])

    # Logic for matching the university names from the universities dataset and
    # the enrollment dataset.  For each row in the enrollment dataset (which
    # has a smaller rowcount), go through each row in the university dataset.
    # If the state abbreviation matches, then check if the set intersection
    # between the name lists created earlier is an exact match for the name
    # list in the enrollment dataset. If so, then create a new record in the
    # join table with the names and ids from each table. Then drop that record
    # from both filtered datasets to ensure they aren't compared going
    # forwards.
    #
    # (So, when the name field in the universities table contains the exact
    # same words as the name in the enrollment table, create a record that
    # associates them in the join table, and drop those records from their
    # original filtered dfs.)
    for enroll_index, enroll_row in enrollment_filtered.iterrows():
        for uni_index, uni_row in universities_filtered.iterrows():
            if enroll_row['State'] == uni_row['state']:
                if (list(set(enroll_row['Medical_School_Split']) & set(uni_row['name_split']))) == enroll_row['Medical_School_Split']:
                    uni_enroll_matched.loc[len(uni_enroll_matched)+1] = [uni_row['id'], uni_row['name'], enroll_row['id'], enroll_row['Medical_School']]
                    enrollment_filtered.drop([enroll_index])
                    universities_filtered.drop([uni_index])

            # Since not all universities in the university dataset have state
            # abbreviations, run the same logic again but with states where the
            # university state data is null or blank. (This comparison logic
            # should probably be a function since it's the exact same.)
            if pd.isnull(uni_row['state']) or uni_row['state'] == '':
                if (list(set(enroll_row['Medical_School_Split']) & set(uni_row['name_split']))) == enroll_row['Medical_School_Split']:
                    uni_enroll_matched.loc[len(uni_enroll_matched)+1] = [uni_row['id'], uni_row['name'], enroll_row['id'], enroll_row['Medical_School']]
                    enrollment_filtered.drop([enroll_index])
                    universities_filtered.drop([uni_index])
    
    # Write the dataset to a CSV for upload to postgres.
    uni_enroll_matched.to_csv('3_generated_files/uni_enroll_matched.csv')

# Run the main function.
if __name__ == "__main__":
    main()
