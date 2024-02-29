import argparse
import subprocess

def parse_arguments():
    '''
    This function is for trim raw fastq files:
    # input fastq
    # output fastq
    :return:
    '''

    parser = argparse.ArgumentParser(
        description='Run qc for trim raw fastq files'
    )
    parser.add_argument('-i', dest="in_dir", help = "Input directory for raw fastq files")
    parser.add_argument('-s', dest='start', help='The start job id')
    parser.add_argument('-e', dest='end', help='The end job id')
    args = parser.parse_args()
    return args

def main():
    args=parse_arguments()
    in_dir = args.in_dir
    start = args.start
    end = args.end
    out_dir = in_dir + '/data/'
    sh_dir = out_dir + '/sh/'
    log_dir = out_dir + '/log/'
    subprocess.call('mkdir -p ' + out_dir + '{sh,log}/',shell=True)
    subprocess.call('mkdir -p ' + out_dir + '/presto/{AssemblePairs,FilterSeq_quality,primer_anno,dedup}', shell=True)
    fout = sh_dir + 'presto_' + start + '_' + end + '.sh'
    log = log_dir + 'presto_' + start + '_' + end + '.log'
    sh = open(fout,"w")
    pt_file = in_dir + '/sample_info/samples.txt'
    pts = open(pt_file)
    lns = pts.readlines()
    cmd = 'echo =================Running QC =============== >>' + log + ' 2>&1 & wait \n'
    cmd += 'echo ============================================================ >>' + log + ' 2>&1 & wait \n'
    cmd += 'echo Authors: Chunlei Yu                        >>' + log + ' 2>&1 & wait \n'
    cmd += 'echo Contact: chunlei.yu1990@gmail.com                      >>' + log + ' 2>&1 & wait \n'
    cmd += 'echo tool version: presto  0.5.10-2018.10.19    >>' + log + ' 2>&1 & wait \n'
    cmd += 'echo ================================================= >>' + log + ' 2>&1 & wait \n'
    for i in range((int)(start) - 1, (int)(end)):
        line = lns[i].split("\t")
        sID = line[0]
        subprocess.call('gunzip ' + out_dir + '/trimmomatic/' + sID + '_R1.trim.fastq.gz ',shell=True)
        subprocess.call('gunzip ' + out_dir + '/trimmomatic/' + sID + '_R2.trim.fastq.gz ', shell=True)
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += 'echo ===========Assembles paired-end reads into a single sequence ' + sID + ' ============ >>' + log + ' 2>&1 & \n\n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/AssemblePairs.py align --maxerror 0.05 --coord illumina --rc tail --nproc 20 '
        cmd += '-1 ' + out_dir + '/trimmomatic/' + sID + '_R1.trim.fastq '
        cmd += '-2 ' + out_dir + '/trimmomatic/' + sID + '_R2.trim.fastq '
        cmd += '--outname ' + sID + ' --outdir ' + out_dir + '/presto/AssemblePairs/'
        cmd += ' --log ' + out_dir + '/presto/AssemblePairs/' + sID + '.AP.log & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += 'echo =================FastQC starts >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/fastqc -t 16 '
        cmd += out_dir + '/presto/AssemblePairs/' + sID + '_assemble-pass.fastq & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/FilterSeq.py quality -q 20 --nproc 20 '
        cmd += '-s ' + out_dir + '/presto/AssemblePairs/' + sID + '_assemble-pass.fastq '
        cmd += ' --outname ' + sID + ' --outdir ' + out_dir + '/presto/FilterSeq_quality/'
        cmd += ' --log ' + out_dir + '/presto/FilterSeq_quality/' + sID + '.FS.log & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += 'echo =================FastQC starts >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/fastqc -t 16 '
        cmd += out_dir + '/presto/FilterSeq_quality/' + sID + '_quality-pass.fastq & wait \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/MaskPrimers.py align --nproc 20 --maxlen 30 --mode mask --pf VPRIMER '
        cmd += '-s ' + out_dir + '/presto/FilterSeq_quality/' + sID + '_quality-pass.fastq '
        cmd += '-p ' + in_dir + '/sample_info/5primer.fasta '
        cmd += '--outname ' + sID + '-FWD '
        cmd += '--outdir ' + out_dir + '/presto/primer_anno/'
        cmd += ' --log ' + out_dir + '/presto/primer_anno/' + sID + '.MPV.log & wait \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/MaskPrimers.py score --nproc 10 '
        cmd += '-s ' + out_dir + '/presto/primer_anno/' + sID + '-FWD_primers-pass.fastq '
        cmd += '-p ' + in_dir + '/sample_info/3primer_keepfr4.fasta '
        cmd += '--mode cut --revpr --pf CPRIMER '
        cmd += '--outname ' + sID + '-REV '
        cmd += '--outdir ' + out_dir + '/presto/primer_anno/'
        cmd += ' --log ' + out_dir + '/presto/primer_anno/' + sID + '.MPC.log & wait \n'
        cmd += 'echo `data` ======================== >>' + log + ' 2>&1 & \n'
        cmd += 'echo =================FastQC starts >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/fastqc -t 16 '
        cmd += out_dir + '/presto/primer_anno/' + sID + '-REV_primers-pass.fastq & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += 'echo =================Removes duplicate sequences from FASTA/FASTQ files >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/CollapseSeq.py -n 20 --inner --uf CPRIMER '
        cmd += '-s ' + out_dir + '/presto/primer_anno/' + sID + '-REV_primers-pass.fastq '
        cmd += '--cf VPRIMER --act set --outname ' + sID + ' '
        cmd += '--outdir ' + out_dir + '/presto/dedup/'
        cmd += ' --log ' + out_dir + '/presto/dedup/' + sID + '.CollapseSeq.log & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
        cmd += 'echo ====== Sorts, samples and splits FASTA/FASTQ sequence files at `$date` >>' + log + ' 2>&1 & \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/SplitSeq.py group '
        cmd += '-s ' + out_dir + '/presto/dedup/' + sID + '_collapse-unique.fastq '
        cmd += '-f DUPCOUNT --num 2 '
        cmd += '--outname ' + sID + ' --fasta --outdir ' + out_dir + '/presto/dedup/ & wait \n'
        cmd += '/rsrch4/home/hema_bio-Malignan/cyu5/.conda/envs/presto/bin/ParseHeaders.py table '
        cmd += '-s ' + out_dir + '/presto/dedup/' + sID + '_atleast-2.fasta '
        cmd += '-f ID DUPCOUNT CPRIMER VPRIMER '
        cmd += '--outdir ' + out_dir + '/presto/dedup/ --outname ' + sID + ' & wait \n'
        cmd += 'echo `date`    ================= >>' + log + ' 2>&1 & \n'
    sh.write(cmd)
    sh.close()
    pts.close()
    cmd1='nohup bash ' + fout + '>>' + log + ' 2>&1 & \n'
    subprocess.call(cmd1, shell=True)

if __name__== '__main__':
    main()

