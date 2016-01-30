Cold Start Recommender
======================

The Cold Start Problem originates from the fact that collaborative
filtering recommenders need data to build recommendations. Typically,
if Users who liked item 'A' also liked item 'B', the recommender would
recommend 'B' to a user who just liked 'A'. But if you have no
previous rating by any User, you cannot make any recommendations.
