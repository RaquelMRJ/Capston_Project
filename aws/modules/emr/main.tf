resource "aws_emr_cluster" "emr-spark-cluster" {
  name          = "Movie review classifier"
  release_label = "emr-5.34.0"         # 2016 November                                                     
  applications  = ["Hadoop", "Spark"]
  service_role = "EMR_DefaultRole" 

  ec2_attributes {
    #key_name  = "${var.key_name}"
    subnet_id = var.subnet_id # "${aws_subnet.main.id}" 
    instance_profile = "EMR_EC2_DefaultRole"                                              
  }

  master_instance_group {
    instance_type = "m5.xlarge"
  }
  
  core_instance_group {
    instance_type  = "m5.xlarge"
    instance_count = 2
    }

  bootstrap_action {
    path = "s3://bootstrap-actions-movies/packages.sh"
    name = "AddPyPackages"
  }
  

}
