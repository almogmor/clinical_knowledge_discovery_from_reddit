#!/bin/bash
#SBATCH --mem=24g
#SBATCH -c 4
#SBATCH --time=2-14:0:0
#SBATCH --gres=gpu:1
#SBATCH --killable
#SBATCH --requeue

module load cuda/10.2

dir=/cs/labs/tomhope/almog.mor/

cd $dir

source /cs/labs/tomhope/almog.mor/new_virtualenv/bin/activate

echo ${SLURM_ARRAY_TASK_ID}

python3.9 add_post_title_author_to_pushitiodataset.py --file_name=raw_data_RC_2020-07
