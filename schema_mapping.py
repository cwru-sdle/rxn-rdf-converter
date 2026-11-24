#%%
import pandas as pd

save_path = "ord_mds_mapping.csv"
file_path = "/home/qdt/CSE_MSE_RXF131/cradle-members/mds3/qdt/git/reaction_proto_ord_terms.csv"

df = pd.read_csv(file_path)
print(df)

df.to_csv(save_path, index=False)
ord_mds_mapping = {
    "Reaction":	"mds:ChemicalReaction",
    "ReactionSetup": "mds:ReactionSetup",
    "ReactionConditions": "mds:ReactionCondition",
    "ReactionNotes": "mds:ReactionNote",
    "ReactionObservations":	"mds:ReactionObservation",
    "ReactionWorkups": "mds:ReactionWorkup",
}
def format_label(text):
    if pd.isna(text):
        return text
    return " ".join(word.capitalize() for word in str(text).split("_"))

df['ord_term'] = df['ord_term'].apply(format_label)
cols_mapping = {
    'ord_term': 'ORD Term',
    'mds_term': 'MDS Class'
}
df = df.rename(columns=cols_mapping)

#%%
import pandas as pd
path = "ord_mds_mapping.csv"
df = pd.read_csv("ord_mds_mapping.csv")

df['Comments'] = ''
print(df.head(100))

df.to_csv(path, index=False)

# %%
