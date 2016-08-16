using Sitecore.Analytics.Data;
using Sitecore.Analytics.DataAccess;
using Sitecore.Analytics.Model;
using Sitecore.Analytics.Model.Entities;
using Sitecore.Analytics.Tracking;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace Website.Tasks
{
    public class ScheduledContactSync
    {
        ContactRepository contactRepository;
        ContactManager contactManager;
        public ScheduledContactSync()
        {
            contactRepository = Factory.CreateObject("tracking/contactRepository", true) as ContactRepository;
            contactManager = Factory.CreateObject("tracking/contactManager", true) as ContactManager;
        }
        private static bool isRunning = false;
        private class DatabaseContact
        {
            public string Email { get; set; }
            public string FirstName { get; set; }
            public string Surname { get; set; }
        }

        public void Run()
        {
            if (isRunning)
            {
                return;
            }
            isRunning = true;
            try
            {
                Assert.IsNotNull(contactRepository, "contactRepository");
                Assert.IsNotNull(contactManager, "contactManager");

                var contacts = new List<DatabaseContact>();
                using (var connection = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["contacts"].ConnectionString))
                {
                    connection.Open();
                    SqlCommand cmd = new SqlCommand(@"SELECT [EmailAddress],
                        [FirstName],
                        [LastName]
                    FROM [Sitecore_core].[dbo].[EmailAddresses]", connection);
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        contacts.Add(new DatabaseContact()
                        {
                            Email = reader["EmailAddress"].ToString(),
                            FirstName = reader["FirstName"].ToString(),
                            Surname = reader["LastName"].ToString(),
                        });
                    }
                }

                foreach (var contact in contacts)
                {
                    var leaseOwner = new LeaseOwner("UpdateContact-" + Guid.NewGuid().ToString(), LeaseOwnerType.OutOfRequestWorker);
                    var c = GetOrCreateContact(contact, leaseOwner);
                    if (c == null)
                    {
                        continue;
                    }

                    var personalInfo = c.Object.GetFacet<IContactPersonalInfo>("Personal");
                    personalInfo.FirstName = contact.FirstName;
                    personalInfo.Surname = contact.Surname;

                    var emailFacet = c.Object.GetFacet<IContactEmailAddresses>("Emails");
                    if (!emailFacet.Entries.Contains("Preferred"))
                    {
                        emailFacet.Entries.Create("Preferred");
                    }
                    emailFacet.Entries["Preferred"].SmtpAddress = contact.Email;
                    emailFacet.Preferred = "Preferred";

                    contactManager.FlushContactToXdb(c.Object);
                    c.Object.ContactSaveMode = ContactSaveMode.AlwaysSave;
                    contactRepository.SaveContact(c.Object, new ContactSaveOptions(true, leaseOwner));
                    contactManager.ReleaseContact(c.Object.ContactId);
                }
            }
            catch(Exception)
            {
            }
            isRunning = false;
        }

        private LockAttemptResult<Contact> GetOrCreateContact(DatabaseContact databaseContact, LeaseOwner leaseOwner)
        {
            var lockAttempt = contactRepository.TryLoadContact(databaseContact.Email, leaseOwner, TimeSpan.FromSeconds(30));

            if (lockAttempt.Status == LockAttemptStatus.NotFound)
            {
                var contact = contactRepository.CreateContact(ID.NewID);
                contact.Identifiers.Identifier = databaseContact.Email;
                contact.System.Value = 0;
                contact.System.VisitCount = 0;
                contact.ContactSaveMode = ContactSaveMode.AlwaysSave;
                contactManager.FlushContactToXdb(contact);
                contactManager.ReleaseContact(contact.ContactId);
                lockAttempt = contactRepository.TryLoadContact(databaseContact.Email, leaseOwner, TimeSpan.FromSeconds(30));
            }
            if (lockAttempt.Status != LockAttemptStatus.Success)
            {
                return null;
            }
            return lockAttempt;
        }
    }
}