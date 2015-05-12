/**
 * This is the skeleton code for bfs.
 * Note the save and load functions in each struct and class are used to serialize
 * and deserialize the object. If you add members with non-primitive data types in
 * a struct or class, you need to implement the save and load functions. For more
 * information, please see the serialization section in the API documentation of
 * GraphLab.
 */

#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

const int MAX_LEN = 15;

/**
 * The vertex data type and also the message type
 */
struct vertex_data {

  // add data members here
  int len;
  int fromId;
  int pageId;
  std::vector<int> route;
  bool changed;

  vertex_data() {
	pageId = -1;
	len = -1;
  }
  
  // GraphLab will use this function to merge messages 
  vertex_data& operator+=(const vertex_data& other) {
        // Fill your code here
		//changed = false;
		if (other.len < len) {
			len = other.len;
			pageId = other.pageId;
			fromId = other.fromId;
			route = other.route;
		}
		else if (other.len == len && other.pageId < pageId) {
			len = other.len;
			pageId = other.pageId;
			fromId = other.fromId;
			route = other.route;
		}
		/*
		if (len < 0) {
			fromId = other.pageId;
			if (fromId < 0) {
				fromId = pageId;
			}
			route = other.route;
			route.push_back(pageId);
			changed = true;
			len = route.size();
		}
		else { 
			if (other.len + 1 < len) {
				len = other.len + 1;
				fromId = other.pageId;
				route = other.route;
				route.push_back(pageId);
				changed = true;
			}
			else if (other.len + 1 == len && fromId < other.pageId) {
				fromId = other.pageId;
				route = other.route;
				route.push_back(pageId);
				changed = true;
			}
        }
		*/
		return *this;
  }

  void applyMsg(const vertex_data& other) {
	changed = false;
	 if (len < 0) {
		fromId = other.pageId;
		if (fromId < 0) {
			fromId = pageId;
		}
		route = other.route;
		route.push_back(pageId);
		changed = true;
		len = route.size();
		}
		else { 
			if (other.len + 1 < len) {
				len = other.len + 1;
				fromId = other.pageId;
				route = other.route;
				route.push_back(pageId);
				changed = true;
			}
			else if (other.len + 1 == len && fromId > other.pageId) {
				fromId = other.pageId;
				route = other.route;
				route.push_back(pageId);
				changed = true;
			}
		}
  }

  void save(graphlab::oarchive& oarc) const {
        // Fill your code here
		oarc << len << fromId << pageId << changed;
		int i = 0;
		for (i = 0; i < route.size(); i++) {
			oarc << route[i];
		}
  }

  void load(graphlab::iarchive& iarc) {
        // Fill your code here
		iarc >> len >> fromId >> pageId >> changed;
		int i = 0, tmp;
		for (i = 0; i < len; i++) {
			iarc >> tmp;
			route.push_back(tmp);
		}
  }
};

/**
 * Definition of graph
 */
typedef graphlab::distributed_graph<vertex_data, graphlab::empty> graph_type;

/* The current id we are processing */
int id;

/**
 * The bfs program.
 */
class bfs :
  public graphlab::ivertex_program<graph_type, 
                                   graphlab::empty,
                                   vertex_data>  {
private:
    vertex_data msg;
public:

  void init(icontext_type& context, const vertex_type& vertex,
            const message_type& msg) {
        this->msg = msg;
        // Fill your code here
  }

  // no gather required
  edge_dir_type gather_edges(icontext_type& context,
                             const vertex_type& vertex) const {
        return graphlab::NO_EDGES;
  }


  void apply(icontext_type& context, vertex_type& vertex,
             const gather_type& gather_result) {
		vertex.data().applyMsg(msg);
      // Fill your code here
  }

  // do scatter on all the in-edges
  edge_dir_type scatter_edges(icontext_type& context,
                             const vertex_type& vertex) const {
        return graphlab::IN_EDGES;
  }

  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
        // Fill your code here
		if (vertex.data().changed) {
			context.signal(edge.source(), vertex.data()); 
		}
  }

  void save(graphlab::oarchive& oarc) const {
        // Fill your code here

  }

  void load(graphlab::iarchive& iarc) {
        // Fill your code here
  }
};

void initialize_vertex(graph_type::vertex_type& vertex) {
    // Fill your code here
    // You should initialize the vertex data here
	vertex.data().len = -1;
	vertex.data().pageId = vertex.id();
    vertex.data().fromId = -1;	
	vertex.data().route.clear();
}

struct shortest_path_writer {
  std::string save_vertex(const graph_type::vertex_type& vtx) {
    // You should print the shortest path here
	std::stringstream strm;

	if (vtx.data().len > 0) {
	
		if (vtx.data().pageId == vtx.data().fromId) {
			int id = vtx.data().pageId;
			strm << id << "\t" << id << "\t" << id << " " << id;
		}
		else if (vtx.data().route.size() > 0) {
			int dest = vtx.data().route[0];
			strm << vtx.data().pageId << "\t" << dest << "\t"; 
			int idx = vtx.data().route.size() - 1;
			strm << vtx.data().route[idx--];
			for (;idx >= 0; idx--) {
				strm << " " << vtx.data().route[idx];
			}	
		}

		strm << "\n";
	}	
	return strm.str();
  }

  std::string save_edge(graph_type::edge_type e) { return ""; }
};

int main(int argc, char** argv) {
  // Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

  // Parse command line options -----------------------------------------------
  graphlab::command_line_options
    clopts("bfs algorithm");
  std::string graph_dir;
  std::string format = "snap";
  std::string saveprefix;
  std::string top_ids;

  clopts.attach_option("graph", graph_dir,
                       "The graph file.");
  clopts.attach_option("format", format,
                       "graph format");
  clopts.attach_option("top", top_ids,
                       "The file which contains top 10 ids");
  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the result to a "
                       "sequence of files with prefix saveprefix");

  if(!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }


  // Build the graph
  graph_type graph(dc, clopts);

  dc.cout() << "Loading graph in format: "<< format << std::endl;
  graph.load_format(graph_dir, format);

  // must call finalize before querying the graph
  graph.finalize();
  dc.cout() << "#vertices:  " << graph.num_vertices() << std::endl
            << "#edges:     " << graph.num_edges() << std::endl;

  graphlab::synchronous_engine<bfs> engine(dc, graph, clopts);
  char id_str[MAX_LEN];
  std::ifstream fin(top_ids.c_str());
  while (fin >> id) {
    graph.transform_vertices(initialize_vertex);

    /*
     * add your implementation here
     */
	engine.signal(id, vertex_data());
	engine.start();


    std::string tmp = saveprefix;
    tmp += '_';
    sprintf(id_str, "%d", id);
    tmp += id_str;
    graph.save(tmp,
            shortest_path_writer(),
            false,   // do not gzip
            true,    // save vertices
            false,   // do not save edges
            1);      // one output file per machine
  }
  fin.close();

  // Tear-down communication layer and quit
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
} // End of main
