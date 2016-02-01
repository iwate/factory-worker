namespace FactoryWorker.Sample
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data.Entity.Spatial;

    [Table("BuildVersion")]
    public partial class BuildVersion
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public byte SystemInformationID { get; set; }

        [Column("Database Version")]
        [Required]
        [StringLength(25)]
        public string Database_Version { get; set; }

        public DateTime VersionDate { get; set; }

        public DateTime ModifiedDate { get; set; }
    }
}
