import os
from statistics import mean
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

LATENCY_FILES = ["hello-latency-cdf.csv",
                 "seqlong-latency-cdf.csv",
                #  "bank-latency-cdf.csv",
                 "img-latency-cdf.csv"
                 ]

THROUGHPUT_FILES = ["hello-throughput.csv",
                    "bank-throughput.csv"]

THROUGHPUT_BAR_FILES = ["hello-throughput-bar.csv",
                        "bank-throughput-bar.csv"]

SCALEUP_FILE = "scaleup-throughput.csv"

DATA_DIR = "data"

PRETTY_PREFIX_NAMES = {"hello-latency": "Hello3",
                       "seqlong-latency": "Sequence (n=10)",
                       "bank-latency": "Bank Application",
                       "img-latency": "Image Recognition",
                       "hello-throughput": "Hello Sequence",
                       "bank-throughput": "Bank Application",}

PRETTY_CONF_NAMES = {"aws": "Step Functions",
                     "neth-np": "Neth. (No Pipe.)",
                     "neth-p": "Netherite",
                     "orig": "Original DF",
                     "blob-af": "Blob Seq. Azure",
                     "trig-aws": "Trig. Seq. AWS",
                     "queue-af": "Queue Seq. Azure",
                     "orch-orig": "Original DF",
                     "orch-neth-np": "Neth. (No Pipe.)",
                     "orch-neth-p": "Netherite",
                     }

CONF_COLORS =  {"aws": "tab:cyan",
                "neth-np": "tab:green",
                "neth-p": "tab:orange",
                "orig": "tab:red",
                "blob-af": "tab:purple",
                "trig-aws": "tab:pink",
                "queue-af": "tab:brown",
                "orch-orig": "tab:red",
                "orch-neth-np": "tab:green",
                "orch-neth-p": "tab:orange"
                }


# plt.rcParams.update({'font.size': 14})

# SMALL_SIZE = 22
# MEDIUM_SIZE = 24
# BIGGER_SIZE = 30

# plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
# plt.rc('axes', titlesize=MEDIUM_SIZE)     # fontsize of the axes title
# plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
# plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
# plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
# plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
# plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

# plt.rcParams['mathtext.fontset'] = 'stix'
# plt.rcParams['font.family'] = 'STIXGeneral'


def read_lines(data_file):
    data_file_path = os.path.join(DATA_DIR, data_file)

    with open(data_file_path) as f:
        data = f.readlines()

    header = data[0]
    lines = [line.rstrip().split(",") for line in data[1:]]
    return lines

def preprocess_latency_data(data_file, filter_ids=["neth+gs","neth-gs","orch-neth+gs","orch-neth-gs"]):
    lines = read_lines(data_file)
    
    grouped_data = {}
    statistics = {}
    for x, y, id in lines:
        short_id = "-".join(id.split(" ")[0].split("-")[1:])
        if not short_id in filter_ids:
            try:
                grouped_data[short_id].append((float(x),float(y)))
            except:
                grouped_data[short_id] = [(float(x),float(y))]
            
            ## Gather statistics
            if(not short_id in statistics):
                statistics[short_id] = { "median" : (0, 101),
                                        "90th" : (0, 101),
                                        "95th" : (0, 101)}
            
            if (abs(float(y) - 50) < statistics[short_id]["median"][1]):
                statistics[short_id]["median"] = (x, abs(float(y) - 50))
            elif (abs(float(y) - 90) < statistics[short_id]["90th"][1]):
                statistics[short_id]["90th"] = (x, abs(float(y) - 90))
            elif (abs(float(y) - 95) < statistics[short_id]["95th"][1]):
                statistics[short_id]["95th"] = (x, abs(float(y) - 95))    
    
    print("Statistics for", data_file)
    for conf in statistics.keys():
        print("|--", conf)
        print("   |-- {} : {} (err: {})".format("median", statistics[conf]["median"][0], statistics[conf]["median"][1]))
        print("   |-- {} : {} (err: {})".format("90th", statistics[conf]["90th"][0], statistics[conf]["90th"][1]))
        print("   |-- {} : {} (err: {})".format("95th", statistics[conf]["95th"][0], statistics[conf]["95th"][1]))

    return grouped_data

def preprocess_throughput_data(data_file):
    lines = read_lines(data_file)

    grouped_data = {}
    for time_ms, id, completed, _time, _load in lines:
        short_id = "-".join(id.split(" ")[0].split("-")[1:])
        if (not short_id in grouped_data):
            grouped_data[short_id] = {}
        time = int(time_ms)
        try:
            grouped_data[short_id][time].append(float(completed))
        except:
            grouped_data[short_id][time] = [float(completed)]
    return grouped_data

def preprocess_throughput_bar_data(data_file):
    lines = read_lines(data_file)

    grouped_data = {}
    for throughput, id in lines:
        short_id = "-".join(id.split(" ")[0].split("-")[1:])
        grouped_data[short_id] = float(throughput)
    return grouped_data


def preprocess_scaleup_throughput_data(data_file):
    lines = read_lines(data_file)
    
    grouped_data = {}
    loads = {}
    for time_ms, completed, _timeoriginal, new_load, id in lines:
        short_id = "-".join(id.split(" ")[0].split("-")[1:])
        if (not short_id in grouped_data):
            grouped_data[short_id] = {}
        time = int(time_ms)
        try:
            grouped_data[short_id][time].append(float(completed))
        except:
            grouped_data[short_id][time] = [float(completed)]

        if(short_id in loads):
            assert(float(new_load) == loads[short_id])
        else:
            loads[short_id] = float(new_load)
    return (grouped_data, loads)

def plot_ccdf(data_file):
    prefix = data_file.split(".")[0]
    prefix = "-".join(prefix.split("-")[:-1])
    grouped_data = preprocess_latency_data(data_file)

    # print(grouped_data.keys())

    fig, ax = plt.subplots()

    ## Set the size to small to increase fonts
    fig.set_size_inches(4, 3)
    ax.set_xlabel('Latency (ms)')
    ax.set_ylabel('CDF')
    # ax.set_yscale("log")
    ax.set_xscale("log")
    # ax.tick_params(direction='in')

    for configuration in grouped_data.keys():
        points = grouped_data[configuration]
        xs, ys = [[float(x) for x, _ in points],
                  [float(y) for _, y in points]]
        ## For CCDF
        # new_ys = [1 - (y / 100.0) for y in ys]
        new_ys = ys

        ## Get a pretty name for legend
        try:
            label = PRETTY_CONF_NAMES[configuration]
        except:
            label = configuration
        ax.plot(xs, new_ys, label=label)
        
    ax.legend()
    # plt.title(prefix)
    # plt.legend(loc='upper right')
    plt.legend(loc='lower right')
    plt.grid()
    plt.tight_layout()
    # plt.show()
    output_path = os.path.join("plots", "{}-ccdf.pdf".format(prefix))
    plt.savefig(output_path)

def plot_throughput(data_file):
    prefix = data_file.split(".")[0]
    prefix = "-".join(prefix.split("-")[:-1])
    grouped_data = preprocess_throughput_data(data_file)
    fig, ax = plt.subplots()

    ## Set the size to small to increase fonts
    fig.set_size_inches(6, 3)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Throughput')
    # ax.tick_params(direction='in')
    # ax.set_yscale("log")
    # ax.set_xscale("log")

    for configuration in grouped_data.keys():
        xs = list(grouped_data[configuration].keys())
        xs.sort()
        ys = [mean(grouped_data[configuration][x]) for x in xs]
        new_xs = [float(x) / 1000 for x in xs]
        ## Get a pretty name for legend
        try:
            label = PRETTY_CONF_NAMES[configuration]
        except:
            label = configuration
        ax.plot(new_xs, ys, label=label, linewidth=1)
        
    ax.legend()
    # plt.title(prefix)
    # plt.legend(loc='upper right')
    plt.legend(loc='lower right')
    plt.grid()
    plt.tight_layout()
    # plt.show()
    output_path = os.path.join("plots", "{}-throughput.pdf".format(prefix))
    plt.savefig(output_path)

def plot_one_tile_throughput_bar(confs, data_file, ax):
    prefix = data_file.split(".")[0]
    prefix = "-".join(prefix.split("-")[:-1])
    grouped_data = preprocess_throughput_bar_data(data_file)
    
    confs = [conf for conf in confs if conf in grouped_data.keys()]
    throughputs = [grouped_data[conf] for conf in confs]
    pretty_confs = [PRETTY_CONF_NAMES[conf] for conf in confs]
    xs = list(range(len(confs)))
    rects = ax.barh(xs, throughputs, tick_label=pretty_confs)
    ax.xaxis.grid(True, linestyle='--',
                  color='grey', alpha=.25)
    
    max_width = max([int(rect.get_width()) for rect in rects])

    for rect in rects:
        # Rectangle widths are already integer-valued but are floating
        # type, so it helps to remove the trailing decimal point and 0 by
        # converting width to int type
        width = int(rect.get_width())

        rank_str = width
        # The bars aren't wide enough to print the ranking inside
        if width < float(max_width) / 10:
            # Shift the text to the right side of the right edge
            xloc = 5
            # Black against white background
            clr = 'black'
            align = 'left'
        else:
            # Shift the text to the left side of the right edge
            xloc = -5
            # White on magenta
            clr = 'white'
            align = 'right'

        # Center the text vertically in the bar
        yloc = rect.get_y() + rect.get_height() / 2
        label = ax.annotate(
            rank_str, xy=(width, yloc), xytext=(xloc, 0),
            textcoords="offset points",
            horizontalalignment=align, verticalalignment='center',
            color=clr, weight='bold', clip_on=True, fontsize=13)

    ax.set_title(PRETTY_PREFIX_NAMES[prefix])
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
            ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(14)

    # ax.grid()

def plot_throughput_bar():
    confs = ["df-http",
             "neth-http",
             "neth-http-ns",
             "neth+ns",
             "neth-ns",
             "orch-neth+ns",
             "orch-neth-ns"
             "neth+ls",
             "neth-ls",
             "orch-neth+ls",
             "orch-neth-ls",
             "neth+gs",
             "neth-gs",
             "orch-neth+gs",
             "orch-neth-gs"]

    clean_confs = ["neth-gs",
                   "neth-ls",
                   "neth-ns",
                   "ex"]

    fig = plt.figure()
    gs = fig.add_gridspec(1, 2, wspace=0.1)

    total_lines = []
    ## Plot microbenchmarks
    for i, data_file in enumerate(THROUGHPUT_BAR_FILES):
        ax = fig.add_subplot(gs[i])
        lines = plot_one_tile_throughput_bar(confs, data_file, ax)
        fig.add_subplot(ax)

    axs = fig.get_axes()
    for ax in axs:
        # if(ax.is_first_col()):
            # ax.set_ylabel('Throughput')
        if(ax.is_last_row()):
            ax.set_xlabel('Throughput')
        if(not ax.is_first_col()):
            ax.set_yticklabels([])
        # ax.label_outer()

    # plt.title(pretty_names[experiment])

    fig.set_size_inches(8, 3)
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "throughput-bars.pdf"),bbox_inches='tight')


def plot_scaleup(data_file):
    grouped_data, loads = preprocess_scaleup_throughput_data(data_file)

    filtered_confs = ["1-2"]

    fig, ax = plt.subplots()

    ## Set the size to small to increase fonts
    fig.set_size_inches(6, 3)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Throughput')
    # ax.tick_params(direction='in')

    for configuration in grouped_data.keys():
        if(not configuration in filtered_confs):
            xs = list(grouped_data[configuration].keys())
            xs.sort()
            ys = [mean(grouped_data[configuration][x]) for x in xs]
            new_xs = [float(x) / 1000 for x in xs]
            ## Get a pretty name for legend
            try:
                label = PRETTY_CONF_NAMES[configuration]
            except:
                label = configuration
            ax.plot(new_xs, ys, label=label, linewidth=1)

            ## Plot load line
            # load_ys = [loads[configuration] for x in new_xs]
            # load_label = "Load " + label
            # ax.plot(new_xs, load_ys, '--', label=load_label, linewidth=1)
        
    ax.legend()
    # plt.title(prefix)
    # plt.legend(loc='upper right')
    plt.legend(loc='lower right')
    plt.grid()
    plt.tight_layout()
    # plt.show()
    output_path = os.path.join("plots", "scaleup-throughput.pdf")
    plt.savefig(output_path)

def plot_one_tile_latencies(confs, data_file, ax):
    prefix = data_file.split(".")[0]
    prefix = "-".join(prefix.split("-")[:-1])
    grouped_data = preprocess_latency_data(data_file)
    ax.set_xscale("log")
    ax.set_ylim([0,105])
    lines = []
    for configuration in grouped_data.keys():
        points = grouped_data[configuration]
        xs, ys = [[float(x) for x, _ in points],
                [float(y) for _, y in points]]
        ## For CCDF
        # new_ys = [1 - (y / 100.0) for y in ys]
        new_ys = ys

        ## Get a pretty name for legend
        try:
            label = PRETTY_CONF_NAMES[configuration]
        except:
            label = configuration
        line, = ax.plot(xs, new_ys, label=label, color=CONF_COLORS[configuration], 
                        linewidth=1)
        lines.append(line)
    # ax.text(.5,.91,prefix, horizontalalignment='center', transform=ax.transAxes)
    ax.set_title(PRETTY_PREFIX_NAMES[prefix],fontsize=10)
    ax.grid()
    return lines


def plot_tiling_latencies():

    confs = ["neth-p",
             "orch-neth-p",
             "neth-np",
             "orch-neth-np",
             "orig",
             "orch-orig",
             "aws",
             "blob-af",
             "queue-af",
             "trig-aws"]

    clean_confs = ["neth-p",
                   "neth-np",
                   "orig",
                   "aws",
                   "blob-af",
                   "queue-af",
                   "trig-aws"]

    fig = plt.figure()
    gs = fig.add_gridspec(1, 3, hspace=0.3)
    # fig.suptitle('')

    # averages = [[] for _ in all_scaleup_numbers]
    ## Plot microbenchmarks
    for i, data_file in enumerate(LATENCY_FILES):
        ax = fig.add_subplot(gs[i])
        _lines = plot_one_tile_latencies(confs, data_file, ax)
        if (i == 2):
            ax.set_xlim([1000, 5000])
        fig.add_subplot(ax)


    # for i, experiment in enumerate(experiments):
    #     ax = fig.add_subplot(gs[i])
    #     _, lines, best_result = collect_scaleup_times_common(experiment, all_scaleup_numbers, results_dir, custom_scaleup_plots, ax)
    #     if(experiment == "double_sort"):
    #         total_lines = lines + total_lines
    #     # elif(experiment == "bigrams"):
    #     #     total_lines += [lines[0]]
    #     ax.set_xticks(all_scaleup_numbers[1:])
    #     ax.text(.5,.91,pretty_names[experiment],
    #     horizontalalignment='center',
    #     transform=ax.transAxes)
    #     # ax.set_yticks([])
    #     fig.add_subplot(ax)

    #     ## Update averages
    #     for i, res in enumerate(best_result):
    #         averages[i].append(res)


    axs = fig.get_axes()
    for ax in axs:
        if(ax.is_first_col()):
            ax.set_ylabel('CDF')
        if(ax.is_last_row()):
            ax.set_xlabel('Latency (ms)')
        # if(ax.is_first_row()):
        #     ax.set_xticklabels([])
        # ax.label_outer()

    legend_lines = [Line2D([0], [0], color=CONF_COLORS[conf], lw=4) for conf in clean_confs]
    legend_labels = [PRETTY_CONF_NAMES[conf] for conf in clean_confs]
    plt.legend(legend_lines, legend_labels, loc='lower right', fontsize=8)

    # plt.title(pretty_names[experiment])

    fig.set_size_inches(12, 1.9)
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "tiling_latencies.pdf"),bbox_inches='tight')

    ## Print average, geo-mean
    # one_liner_averages = [sum(res)/len(res) for res in averages]
    # geo_means = [math.exp(np.log(res).sum() / len(res))
    #              for res in averages]
    # print("One-liners Aggregated results:")
    # print(" |-- Averages:", one_liner_averages)
    # print(" |-- Geometric Means:", geo_means)




# for data_file in LATENCY_FILES:
#     plot_ccdf(data_file)

# for data_file in THROUGHPUT_FILES:
#     plot_throughput(data_file)

plot_tiling_latencies()

plot_ccdf("bank-latency-cdf.csv")

# plot_throughput_bar()

# plot_scaleup(SCALEUP_FILE)