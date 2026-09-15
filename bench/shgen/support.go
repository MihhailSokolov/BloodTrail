// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
)

// Kind tags for guid derivation -- distinct namespaces so an OU's GUID can
// never collide with a GPO's.
const (
	gpoKind = iota + 1
	ouKind
	containerKind
)

const gposPerDomain = 3

var ouNames = []string{"DOMAIN CONTROLLERS", "WORKSTATIONS", "SERVERS", "EMPLOYEES"}

var containerNames = []string{"USERS", "COMPUTERS"}

// aclRights are the group-appropriate ACL edge kinds the noise generator
// draws from; each is an edge BloodHound renders and can path through.
var aclRights = []string{"GenericAll", "GenericWrite", "WriteDacl", "WriteOwner", "Owns"}

var jobTitles = []string{
	"Accountant", "Sales Representative", "Software Engineer", "HR Specialist",
	"Marketing Manager", "Support Technician", "Data Analyst", "Project Manager",
	"Operations Coordinator", "Legal Counsel", "Procurement Officer", "Designer",
}

func jobTitle(i int) string { return jobTitles[i%len(jobTitles)] }

var osNames = []string{
	"Windows 11 Enterprise", "Windows 10 Enterprise", "Windows Server 2022 Standard",
	"Windows Server 2019 Standard", "Windows 11 Pro",
}

func osName(i int) string { return osNames[i%len(osNames)] }

// firstNames/lastNames give user accounts corporate-looking sAMAccountNames
// (FIRST.LASTNNNNN) while staying fully synthetic; the numeric suffix keeps
// every name unique at any scale.
var firstNames = []string{
	"ANNA", "BEN", "CARLA", "DAVID", "ELENA", "FELIX", "GRETA", "HENRIK",
	"INES", "JONAS", "KIRA", "LUKAS", "MARIA", "NIKO", "OLGA", "PETER",
	"QUINN", "RITA", "SAMUEL", "TANJA", "URSULA", "VICTOR", "WANDA", "XAVER",
	"YVONNE", "ZOLTAN", "AMIR", "BIRGIT", "CHEN", "DANA", "EMIL", "FIONA",
}

var lastNames = []string{
	"SMITH", "JOHNSON", "MILLER", "GARCIA", "MUELLER", "KOWALSKI", "ROSSI", "TANAKA",
	"NOVAK", "JANSEN", "SILVA", "POPOV", "NGUYEN", "HANSEN", "VIRTANEN", "HORVATH",
	"COSTA", "DUBOIS", "WEBER", "LINDQVIST", "MORETTI", "PETROV", "KELLER", "BAKKER",
	"SANTOS", "FISCHER", "ANDERSEN", "KOVACS", "RIVERA", "SCHMIDT", "OKAFOR", "LARSEN",
}

// userSam derives user (d, ui)'s sAMAccountName. The reserved
// attack-foothold indices get recognizable service/staff names so a graph
// explorer can spot them; everyone else cycles the name lists with a unique
// numeric suffix.
func userSam(d, ui int) string {
	switch ui {
	case auKerberoastable:
		return fmt.Sprintf("SVC-SQL%02d", d)
	}
	f := firstNames[(ui+d*7)%len(firstNames)]
	l := lastNames[(ui/len(firstNames)+d*3)%len(lastNames)]
	return fmt.Sprintf("%s.%s%05d", f, l, ui)
}

// groupName names group (d, gi): the reserved attack-path groups get their
// tier names, everything else becomes a department/team-style group.
func groupName(d, gi int) string {
	switch gi {
	case agServerAdmins:
		return "SERVER ADMINS"
	case agITAdmins:
		return "IT ADMINS"
	case agHelpdesk:
		return "HELPDESK"
	case agAppOwners:
		return "APP OWNERS"
	}
	dept := []string{"FINANCE", "SALES", "ENGINEERING", "HR", "MARKETING", "SUPPORT", "LEGAL", "OPS"}[gi%8]
	return fmt.Sprintf("%s-TEAM-%04d", dept, gi)
}

// domainDN renders "MEGACORP.LOCAL" as "DC=MEGACORP,DC=LOCAL".
func domainDN(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = "DC=" + p
	}
	return strings.Join(parts, ",")
}

// sanitizeDomain turns the forest name into a filename prefix.
func sanitizeDomain(name string) string {
	return strings.ToLower(strings.ReplaceAll(name, ".", "_"))
}

// metaType maps a file kind to the SharpHound meta "type" value (they
// coincide for every kind shgen emits).
func metaType(typ string) string { return typ }
