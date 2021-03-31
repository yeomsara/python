
sns.scatterplot(data=pca_df[pca_df['CLASS_CAT'].isin([2,4])], x="PCA1", y="PCA2", hue="CLASS_CAT", style="CLASS_CAT")
