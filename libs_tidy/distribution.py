import matplotlib.pyplot as plt


def plot_distribution(group, month): 
    counts = group['create_day'].value_counts().sort_index()
    # Test of the distribution
    plt.figure(figsize=(8,3))
    counts.plot(kind='bar')
    plt.title(f"Distribution per day – {month}")
    plt.xlabel("Day of month")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.show()