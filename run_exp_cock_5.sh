#!/usr/bin/env bash

export local_ip=192.168.48.184
export ips=(192.168.48.184 192.168.48.185 192.168.48.186 192.168.48.187 192.168.48.188)
export data_dir=/temp/team_p/project-files/data-files/
export xact_dir=/temp/team_p/project-files/xact-files/
export project_dir=/temp/team_p/team_p_coc/
export n_nodes=5

for n_clients in 20 40
do
        ssh xcnc35 "cd $project_dir && git pull && java -jar loadData.jar"
        echo "Reloaded DB"
        ssh xcnc35 "cd $project_dir && git pull && bash run_node_cock.sh $xact_dir 1 $n_clients $n_nodes ${ips[0]}" &
        ssh xcnc36 "cd $project_dir && git pull && bash run_node_cock.sh $xact_dir 2 $n_clients $n_nodes ${ips[1]}" &
        ssh xcnc37 "cd $project_dir && git pull && bash run_node_cock.sh $xact_dir 3 $n_clients $n_nodes ${ips[2]}" &
        ssh xcnc38 "cd $project_dir && git pull && bash run_node_cock.sh $xact_dir 4 $n_clients $n_nodes ${ips[3]}" &
        ssh xcnc39 "cd $project_dir && git pull && bash run_node_cock.sh $xact_dir 5 $n_clients $n_nodes ${ips[4]}" &
        wait
        echo "All nodes finished"
        dbstate_csv=${n_clients}_${n_nodes}_dbstate.csv
        ssh xcnc35 "cd $project_dir && java -jar dbState.jar $dbstate_csv $local_ip"
        echo "Finished experiment $n_clients $n_nodes"

done