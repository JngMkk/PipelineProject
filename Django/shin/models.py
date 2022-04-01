# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class ciaccident(models.Model):
    cahseq = models.IntegerField(primary_key=True)
    cahdate = models.CharField(max_length=10, blank=True, null=True)
    cahcontent = models.CharField(max_length=1000, blank=True, null=True)
    cahaction = models.CharField(max_length=1000, blank=True, null=True)
    cahcode = models.CharField(max_length=10, blank=True, null=True)
    cicode = models.CharField(max_length=20)

    class Meta:
        managed = False
        db_table = 'ciaccident'


class ciall(models.Model):
    ciseq = models.IntegerField(primary_key=True)
    cicode = models.CharField(max_length=20)
    ciname = models.CharField(max_length=255, blank=True, null=True)
    cimingong = models.CharField(max_length=10, blank=True, null=True)
    ciinout = models.CharField(max_length=10, blank=True, null=True)
    ciplacecode = models.CharField(max_length=10, blank=True, null=True)
    ciduty = models.CharField(max_length=10, blank=True, null=True)
    ciopercode = models.CharField(max_length=10, blank=True, null=True)
    ciaddr = models.CharField(max_length=255, blank=True, null=True)
    lat = models.FloatField(blank=True, null=True)
    lng = models.FloatField(blank=True, null=True)
    ctmnotidate = models.CharField(max_length=10, blank=True, null=True)
    ctmreviewat = models.CharField(max_length=5, blank=True, null=True)
    cifloorcode = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ciall'
        unique_together = (('ciseq', 'cicode'),)


class cicahcause(models.Model):
    cahcode = models.CharField(primary_key=True, max_length=10)
    cahcodename = models.CharField(db_column='cahcodeName', max_length=20)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'cicahcause'


class cifloorinfo(models.Model):
    cifloorcode = models.CharField(primary_key=True, max_length=10)
    flocodename = models.CharField(db_column='flocodeName', max_length=10)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'cifloorinfo'


class ciinst(models.Model):
    caseq = models.IntegerField(primary_key=True)
    ciseq = models.IntegerField(blank=True, null=True)
    cainstno = models.CharField(max_length=10, blank=True, null=True)
    cacuser = models.CharField(max_length=20, blank=True, null=True)
    cacdate = models.CharField(max_length=10, blank=True, null=True)
    ciinstcode = models.CharField(max_length=10, blank=True, null=True)
    ciinst = models.CharField(max_length=30, blank=True, null=True)
    cainstcode = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'ciinst'


class cimanager(models.Model):
    csseq = models.IntegerField(primary_key=True)
    ciseq = models.IntegerField(blank=True, null=True)
    ciindgroup = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'cimanager'


class cioperation(models.Model):
    ciopercode = models.CharField(primary_key=True, max_length=10)
    opcodename = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'cioperation'


class ciplaceinfo(models.Model):
    ciplacecode = models.CharField(max_length=10, blank=True, null=True)
    plcodename = models.CharField(primary_key=True, max_length=10)

    class Meta:
        managed = False
        db_table = 'ciplaceinfo'
