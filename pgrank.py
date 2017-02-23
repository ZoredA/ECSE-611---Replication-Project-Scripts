from __future__ import print_function, division

import pandas as pd
import networkx as nx
from networkx.drawing.nx_pydot import write_dot

import matplotlib.pyplot as plt
from matplotlib import patches
#%matplotlib inline
import seaborn as sns
import operator
import numpy as np
sns.set_context('notebook', font_scale=1.5)
sns.set_style('white')

def make_graph(df, min_edges=0):
    DG = nx.DiGraph()
    DG.add_nodes_from(df.package.unique())
    edges = df.loc[df.dependency.notnull(), ['package', 'dependency']].values
    DG.add_edges_from(edges)
    
    # Remove bad nodes
    DG.remove_nodes_from(['.', 'nan', np.nan])
    
    deg = DG.degree()
    to_remove = [n for n in deg if deg[n] <= min_edges]
    DG.remove_nodes_from(to_remove)
    return DG
    
print('a')    

def my_run(export_name = 'pypirank_export.csv'):
    requirements = pd.read_csv('tempDeps.csv')
    dep_graph = make_graph(requirements, min_edges=0)
    pr = nx.link_analysis.pagerank_scipy(dep_graph)
    sorted_dict = sorted(pr.items(), key=operator.itemgetter(1))[::-1]
    with open(export_name,'w') as f:
        for key in sorted_dict:
            f.write('%s,%s\n'% key )

def just_get_dict(input_csv):
    requirements = pd.read_csv(input_csv)
    dep_graph = make_graph(requirements, min_edges=0)
    pr = nx.link_analysis.pagerank_scipy(dep_graph)
    sorted_dict = sorted(pr.items(), key=operator.itemgetter(1))[::-1]
    return sorted_dict

def run():
    # Make a dotfile to import into gephi and make the network graph
    DG = make_graph(requirements, min_edges=10)
    write_dot(DG, 'require_graph.dot')

    dep_graph = make_graph(requirements, min_edges=0)

    len(dep_graph.node)

    sorted_dict = sorted(dep_graph.in_degree().items(), key=operator.itemgetter(1))[::-1]

    N = 10
    x = np.arange(N)
    y = np.array([d[1] for d in sorted_dict[:N]])
    xlabels = [d[0] for d in sorted_dict[:N]][::-1]
    fig, ax = plt.subplots(1, 1, figsize=(7, 7))

    ax.barh(x[::-1], y, height=1.0)
    ax.set_yticks(x + 0.5)
    _ = ax.set_yticklabels(xlabels)
    ax.set_xlabel('Number of Connections')
    ax.set_title('Graph Degree')
    fig.subplots_adjust(left=0.27, bottom=0.1, top=0.95)

    fig.savefig('Figures/Connections.png')

    pr = nx.link_analysis.pagerank_scipy(dep_graph)
    sorted_dict = sorted(pr.items(), key=operator.itemgetter(1))[::-1]


    N = 10
    x = np.arange(N)
    y = np.array([d[1] for d in sorted_dict[:N]])
    xlabels = [d[0] for d in sorted_dict[:N]][::-1]
    xlabels[0] = 'sphinx-py3doc-\nenhanced-theme'
    fig, ax = plt.subplots(1, 1, figsize=(7, 7))

    ax.barh(x[::-1], y, height=1.0)
    ax.set_yticks(x + 0.5)
    _ = ax.set_yticklabels(xlabels)
    ax.set_xlabel('PageRank')
    ax.set_title('Graph Connectivity')
    fig.subplots_adjust(left=0.30, bottom=0.1, top=0.95)

    fig.savefig('Figures/PageRank.png')

    deg = dep_graph.degree()

    bins=30
    fig, ax = plt.subplots(1, 1, figsize=(11,4))
    ax.hist(deg.values(), bins=bins, normed=False)
    ax.plot(ax.get_xlim(), [1, 1], 'k--', alpha=0.5)
    ax.set_xlabel('Degree')
    ax.set_ylabel('Number')
    ax.set_title('Degree Distribution')
    ax.set_yscale('log')
    ax.set_ylim((0.5, 1e5))

    fig.subplots_adjust(left=0.1, bottom=0.15)
    fig.savefig('Figures/DegreeDistribution.png')

    bc = nx.betweenness_centrality(dep_graph)

    sorted_dict = sorted(bc.items(), key=operator.itemgetter(1))[::-1]

    N = 10
    x = np.arange(N)
    y = np.array([d[1]*100 for d in sorted_dict[:N]])
    xlabels = [d[0] for d in sorted_dict[:N]][::-1]
    fig, ax = plt.subplots(1, 1, figsize=(7, 7))

    ax.barh(x[::-1], y, height=1.0)
    ax.set_yticks(x + 0.5)
    _ = ax.set_yticklabels(xlabels)
    ax.set_xlabel('Betweenness Connectivity (%)')
    fig.subplots_adjust(left=0.32, bottom=0.1, top=0.95)

    fig.savefig('Figures/Betweenness.png')


 

