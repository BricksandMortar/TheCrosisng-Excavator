// <copyright>
// Copyright 2013 by the Spark Development Network
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//

using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;

namespace Excavator.F1
{
    /// <summary>
    /// Partial of F1Component that holds the Financial import methods
    /// </summary>
    public partial class F1Component
    {
        /// <summary>
        /// Maps the account data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <returns></returns>
        private void MapBankAccount( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var importedBankAccounts = new FinancialPersonBankAccountService( lookupContext ).Queryable().AsNoTracking().ToList();
            var newBankAccounts = new List<FinancialPersonBankAccount>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying check number import ({0:N0} found, {1:N0} already exist).", totalRows, importedBankAccounts.Count ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                int? individualId = row["Individual_ID"] as int?;
                int? householdId = row["Household_ID"] as int?;
                var personKeys = GetPersonKeys( individualId, householdId, false );
                if ( personKeys != null && personKeys.PersonAliasId > 0 )
                {
                    int? routingNumber = row["Routing_Number"] as int?;
                    string accountNumber = row["Account"] as string;
                    if ( routingNumber != null && !string.IsNullOrWhiteSpace( accountNumber ) )
                    {
                        accountNumber = accountNumber.Replace( " ", string.Empty );
                        string encodedNumber = FinancialPersonBankAccount.EncodeAccountNumber( routingNumber.ToString().PadLeft( 9, '0' ), accountNumber );
                        if ( !importedBankAccounts.Any( a => a.PersonAliasId == personKeys.PersonAliasId && a.AccountNumberSecured == encodedNumber ) )
                        {
                            var bankAccount = new FinancialPersonBankAccount();
                            bankAccount.CreatedByPersonAliasId = ImportPersonAliasId;
                            bankAccount.CreatedDateTime = ImportDateTime;
                            bankAccount.ModifiedDateTime = ImportDateTime;
                            bankAccount.AccountNumberSecured = encodedNumber;
                            bankAccount.AccountNumberMasked = accountNumber.ToString().Masked();
                            bankAccount.PersonAliasId = ( int ) personKeys.PersonAliasId;

                            newBankAccounts.Add( bankAccount );
                            completed++;
                            if ( completed % percentage < 1 )
                            {
                                int percentComplete = completed / percentage;
                                ReportProgress( percentComplete, string.Format( "{0:N0} numbers imported ({1}% complete).", completed, percentComplete ) );
                            }
                            else if ( completed % ReportingNumber < 1 )
                            {
                                SaveBankAccounts( newBankAccounts );
                                newBankAccounts.Clear();
                                ReportPartialProgress();
                            }
                        }
                    }
                }
            }

            if ( newBankAccounts.Any() )
            {
                SaveBankAccounts( newBankAccounts );
            }

            ReportProgress( 100, string.Format( "Finished check number import: {0:N0} numbers imported.", completed ) );
        }

        /// <summary>
        /// Saves the bank accounts.
        /// </summary>
        /// <param name="newBankAccounts">The new bank accounts.</param>
        private static void SaveBankAccounts( List<FinancialPersonBankAccount> newBankAccounts )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPersonBankAccounts.AddRange( newBankAccounts );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Maps the batch data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void MapBatch( IQueryable<Row> tableData )
        {
            var batchStatusClosed = Rock.Model.BatchStatus.Closed;
            var newBatches = new List<FinancialBatch>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying batch import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedBatches.Count ) );
            foreach ( var row in tableData.Where( r => r != null ) )
            {
                int? batchId = row["BatchID"] as int?;
                if ( batchId != null && !ImportedBatches.ContainsKey( ( int ) batchId ) )
                {
                    var batch = new FinancialBatch();
                    batch.CreatedByPersonAliasId = ImportPersonAliasId;
                    batch.ForeignKey = batchId.ToString();
                    batch.ForeignId = batchId;
                    batch.Note = string.Empty;
                    batch.Status = batchStatusClosed;
                    batch.AccountingSystemCode = string.Empty;

                    string name = row["BatchName"] as string;
                    if ( name != null )
                    {
                        name = name.Trim();
                        batch.Name = name.Left( 50 );
                        batch.CampusId = CampusList.Where( c => name.StartsWith( c.Name ) || name.StartsWith( c.ShortCode ) )
                            .Select( c => ( int? ) c.Id ).FirstOrDefault();
                    }

                    DateTime? batchDate = row["BatchDate"] as DateTime?;
                    if ( batchDate != null )
                    {
                        batch.BatchStartDateTime = batchDate;
                        batch.BatchEndDateTime = batchDate;
                    }

                    decimal? amount = row["BatchAmount"] as decimal?;
                    if ( amount != null )
                    {
                        batch.ControlAmount = amount.HasValue ? amount.Value : new decimal();
                    }

                    newBatches.Add( batch );
                    completed++;
                    if ( completed % percentage < 1 )
                    {
                        int percentComplete = completed / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} batches imported ({1}% complete).", completed, percentComplete ) );
                    }
                    else if ( completed % ReportingNumber < 1 )
                    {
                        SaveFinancialBatches( newBatches );
                        newBatches.ForEach( b => ImportedBatches.Add( ( int ) b.ForeignId, ( int? ) b.Id ) );
                        newBatches.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            // add a default batch to use with contributions
            if ( !ImportedBatches.ContainsKey( 0 ) )
            {
                var defaultBatch = new FinancialBatch();
                defaultBatch.CreatedDateTime = ImportDateTime;
                defaultBatch.CreatedByPersonAliasId = ImportPersonAliasId;
                defaultBatch.Status = Rock.Model.BatchStatus.Closed;
                defaultBatch.Name = string.Format( "Default Batch (Imported {0})", ImportDateTime );
                defaultBatch.ControlAmount = 0.0m;
                defaultBatch.ForeignKey = "0";
                defaultBatch.ForeignId = 0;

                newBatches.Add( defaultBatch );
            }

            if ( newBatches.Any() )
            {
                SaveFinancialBatches( newBatches );
                newBatches.ForEach( b => ImportedBatches.Add( ( int ) b.ForeignId, ( int? ) b.Id ) );
            }

            ReportProgress( 100, string.Format( "Finished batch import: {0:N0} batches imported.", completed ) );
        }

        /// <summary>
        /// Saves the financial batches.
        /// </summary>
        /// <param name="newBatches">The new batches.</param>
        private static void SaveFinancialBatches( List<FinancialBatch> newBatches )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialBatches.AddRange( newBatches );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Maps the contribution.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="selectedColumns">The selected columns.</param>
        private void MapContribution( IQueryable<Row> tableData, List<string> selectedColumns = null )
        {
            var lookupContext = new RockContext();
            //            int transactionEntityTypeId = EntityTypeCache.Read( "Rock.Model.FinancialTransaction" ).Id;
            int transactionTypeContributionId = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.TRANSACTION_TYPE_CONTRIBUTION ), lookupContext ).Id;

            var currencyTypes = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE ) );
            // ReSharper disable once InconsistentNaming
            int currencyTypeACH = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_ACH ) ) ).Id;
            int currencyTypeCash = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CASH ) ) ).Id;
            int currencyTypeCheck = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CHECK ) ) ).Id;
            int currencyTypeCreditCard = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CREDIT_CARD ) ) ).Id;
            var currencyTypeNonCash = currencyTypes.DefinedValues.Where( dv => dv.Value.Equals( "Non-Cash" ) ).Select( dv => ( int? ) dv.Id ).FirstOrDefault();
            if ( currencyTypeNonCash == null )
            {
                var newTenderNonCash = new DefinedValue();
                newTenderNonCash.Value = "Non-Cash";
                newTenderNonCash.Description = "Non-Cash";
                newTenderNonCash.DefinedTypeId = currencyTypes.Id;
                lookupContext.DefinedValues.Add( newTenderNonCash );
                lookupContext.SaveChanges();

                currencyTypeNonCash = newTenderNonCash.Id;
            }

            var creditCardTypes = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE ) ).DefinedValues;

            int sourceTypeOnsite = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_ONSITE_COLLECTION ), lookupContext ).Id;
            int sourceTypeWebsite = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_WEBSITE ), lookupContext ).Id;
            int sourceTypeKiosk = DefinedValueCache.Read( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_KIOSK ), lookupContext ).Id;

            var refundReasons = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_REFUND_REASON ), lookupContext ).DefinedValues;

            var accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();

            int? defaultBatchId = null;
            if ( ImportedBatches.ContainsKey( 0 ) )
            {
                defaultBatchId = ImportedBatches[0];
            }

            // Get all imported contributions
            var importedContributions = new FinancialTransactionService( lookupContext ).Queryable().AsNoTracking()
               .Where( c => c.ForeignId != null )
               // ReSharper disable once PossibleInvalidOperationException
               .ToDictionary( t => t.ForeignId.Value, t => t.Id );

            // List for batching new contributions
            var newTransactions = new List<FinancialTransaction>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, $"Verifying contribution import ({totalRows:N0} found, {importedContributions.Count:N0} already exist)." );
            foreach ( var row in tableData.Where( r => r != null ) )
            {
                var individualId = row["Individual_ID"] as int?;
                var householdId = row["Household_ID"] as int?;
                var contributionId = row["ContributionID"] as int?;

                if ( contributionId == null || importedContributions.ContainsKey( contributionId.Value ) || newTransactions.Any( nt => nt.ForeignId == contributionId.Value ) )
                {
                    continue;
                }

                var transaction = new FinancialTransaction
                {
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    ModifiedByPersonAliasId = ImportPersonAliasId,
                    TransactionTypeValueId = transactionTypeContributionId,
                    ForeignKey = contributionId.ToString(),
                    ForeignId = contributionId
                };

                int? giverAliasId = null;
                var personKeys = GetPersonKeys( individualId, householdId );
                if ( personKeys != null && personKeys.PersonAliasId > 0 )
                {
                    giverAliasId = personKeys.PersonAliasId;
                    transaction.CreatedByPersonAliasId = giverAliasId;
                    transaction.AuthorizedPersonAliasId = giverAliasId;
                    transaction.ProcessedByPersonAliasId = giverAliasId;
                }

                string summary = row["Memo"] as string;
                if ( summary != null )
                {
                    transaction.Summary = summary;
                }

                var batchId = row["BatchID"] as int?;
                if ( batchId != null && ImportedBatches.Any( b => b.Key.Equals( batchId ) ) )
                {
                    transaction.BatchId = ImportedBatches.FirstOrDefault( b => b.Key.Equals( batchId ) ).Value;
                }
                else
                {
                    // use the default batch for any non-matching transactions
                    transaction.BatchId = defaultBatchId;
                }

                var receivedDate = row["Received_Date"] as DateTime?;
                if ( receivedDate != null )
                {
                    transaction.TransactionDateTime = receivedDate;
                    transaction.CreatedDateTime = receivedDate;
                    transaction.ModifiedDateTime = ImportDateTime;
                }

                string cardType = row["Card_Type"] as string;
                string cardLastFour = row["Last_Four"] as string;

                string checkNumber = row["Check_Number"] as string;
                string contributionType = row["Contribution_Type_Name"].ToStringSafe().ToLower();
                if ( contributionType != null )
                {
                    // set default source to onsite, exceptions listed below
                    transaction.SourceTypeValueId = sourceTypeOnsite;

                    int? paymentCurrencyTypeId = null, creditCardTypeId = null;

                    switch ( contributionType )
                    {
                        case "cash":
                            paymentCurrencyTypeId = currencyTypeCash;
                            break;
                        case "check":
                            paymentCurrencyTypeId = currencyTypeCheck;
                            break;
                        case "ach":
                            paymentCurrencyTypeId = currencyTypeACH;
                            if (!string.IsNullOrEmpty(checkNumber))
                            {

                                transaction.SourceTypeValueId = sourceTypeWebsite;
                            }
                            else
                            {
                                transaction.SourceTypeValueId = sourceTypeOnsite;
                            }
                            break;
                        case "credit card":
                            paymentCurrencyTypeId = currencyTypeCreditCard;
                            transaction.SourceTypeValueId = sourceTypeWebsite;

                            if ( cardType != null )
                            {
                                creditCardTypeId = creditCardTypes.Where( t => t.Value.Equals( cardType ) ).Select( t => ( int? ) t.Id ).FirstOrDefault();
                            }
                            break;
                        default:
                            paymentCurrencyTypeId = currencyTypeNonCash;
                            break;
                    }

                    var paymentDetail = new FinancialPaymentDetail
                    {
                        CreatedDateTime = receivedDate,
                        CreatedByPersonAliasId = giverAliasId,
                        ModifiedDateTime = ImportDateTime,
                        ModifiedByPersonAliasId = giverAliasId,
                        CurrencyTypeValueId = paymentCurrencyTypeId,
                        CreditCardTypeValueId = creditCardTypeId,
                        AccountNumberMasked = cardLastFour,
                        ForeignKey = contributionId.ToString(),
                        ForeignId = contributionId
                    };

                    transaction.FinancialPaymentDetail = paymentDetail;
                }

                // if the check number is valid, put it in the transaction code
                if ( checkNumber.AsIntegerOrNull() != null )
                {
                    transaction.TransactionCode = checkNumber;
                }
                // check for SecureGive kiosk transactions
                else if ( !string.IsNullOrEmpty( checkNumber ) && checkNumber.StartsWith( "SG" ) )
                {
                    transaction.SourceTypeValueId = sourceTypeKiosk;
                }

                string fundName = row["Fund_Name"] as string;
                string subFund = row["Sub_Fund_Name"] as string;
                string fundGlAccount = row["Fund_GL_Account"] as string;
                // ReSharper disable once InconsistentNaming
                string subFundGLAccount = row["Sub_Fund_GL_Account"] as string;
                bool isFundActive = row["Fund_Is_active"].ToString().AsBoolean();
                bool subFundIsActive = row["Sub_Fund_Is_active"].ToString().AsBoolean();
                var statedValue = row["Stated_Value"] as decimal?;
                var amount = row["Amount"] as decimal?;
                string fundType = row["FundType"] as string;

                // is active if subfund and fund are active or if fund is active and it's not a subfund
                bool isActive = isFundActive && subFundIsActive || ( !subFundIsActive && string.IsNullOrWhiteSpace( subFund ) );

                if ( fundName != null & amount != null )
                {
                    int transactionAccountId;
                    var parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName ) );
                    if ( parentAccount == null )
                    {
                        parentAccount = AddAccount( lookupContext, fundName, fundGlAccount, null, null, isActive, fundType );
                        accountList.Add( parentAccount );
                    }
                    else if ( !parentAccount.IsActive && isActive )
                    {
                        var existingAccount = lookupContext.FinancialAccounts.FirstOrDefault( a => a.Name == parentAccount.Name );
                        existingAccount.IsActive = true;
                        lookupContext.Entry( existingAccount ).State = EntityState.Modified;
                        lookupContext.SaveChanges( DisableAuditing );

                        accountList.Remove( parentAccount );
                        accountList.Add( existingAccount );
                    }

                    if ( subFund != null )
                    {
                        int? campusFundId = null;
                        // assign a campus if the subfund is a campus fund
                        var campusFund = CampusList.FirstOrDefault( c => subFund.StartsWith( c.Name ) || subFund.StartsWith( c.ShortCode ) );
                        if ( campusFund != null )
                        {
                            // use full campus name as the subfund
                            subFund = campusFund.Name;
                            campusFundId = campusFund.Id;
                        }

                        // add info to easily find/assign this fund in the view
                        subFund = subFund.Truncate( 50 );


                        if ( parentAccount == null )
                        {
                            parentAccount = accountList.FirstOrDefault( a => a.Name == fundName );
                        }

                        var childAccount = accountList.FirstOrDefault( c => c.Name.Equals( subFund ) && c.ParentAccountId == parentAccount.Id );
                        if ( childAccount == null )
                        {
                            // create a child account with a campusId if it was set
                            childAccount = AddAccount( lookupContext, subFund, subFundGLAccount, campusFundId, parentAccount.Id, isActive, fundType );
                            accountList.Add( childAccount );
                        }
                        else if ( !childAccount.IsActive && isActive )
                        {
                            var existingAccount = lookupContext.FinancialAccounts.FirstOrDefault( a => a.Name == childAccount.Name );
                            existingAccount.IsActive = true;
                            lookupContext.Entry( existingAccount ).State = EntityState.Modified;
                            lookupContext.SaveChanges( DisableAuditing );

                            accountList.Remove( parentAccount );
                            accountList.Add( existingAccount );
                        }

                        transactionAccountId = childAccount.Id;
                    }
                    else
                    {
                        transactionAccountId = parentAccount.Id;
                    }

                    if ( amount == 0 && statedValue != null && statedValue != 0 )
                    {
                        amount = statedValue;
                    }

                    var transactionDetail = new FinancialTransactionDetail
                    {
                        Amount = ( decimal ) amount,
                        CreatedDateTime = receivedDate,
                        AccountId = transactionAccountId
                    };
                    transaction.TransactionDetails.Add( transactionDetail );

                    if ( amount < 0 )
                    {
                        transaction.RefundDetails = new FinancialTransactionRefund
                        {
                            CreatedDateTime = receivedDate,
                            RefundReasonValueId =
                                refundReasons.Where( dv => summary != null && dv.Value.Contains( summary ) )
                                             .Select( dv => ( int? ) dv.Id ).FirstOrDefault(),
                            RefundReasonSummary = summary
                        };
                    }
                }

                newTransactions.Add( transaction );
                completed++;
                if ( completed % percentage < 1 )
                {
                    int percentComplete = completed / percentage;
                    ReportProgress( percentComplete,
                        $"{completed:N0} contributions imported ({percentComplete}% complete)." );
                }
                else if ( completed % ReportingNumber < 1 )
                {
                    SaveContributions( newTransactions );

                    // Update transactions to prevent duplicates
                    foreach ( var financialTransaction in newTransactions.Where( nt => nt.ForeignId.HasValue ) )
                    {
                        importedContributions.Add( financialTransaction.ForeignId.Value, financialTransaction.Id );
                    }
                    newTransactions.Clear();
                    lookupContext = new RockContext();

                    ReportPartialProgress();
                }
            }

            if ( newTransactions.Any() )
            {
                SaveContributions( newTransactions );
            }

            ReportProgress( 100, $"Finished contribution import: {completed:N0} contributions imported." );
        }

        /// <summary>
        /// Saves the contributions.
        /// </summary>
        /// <param name="newTransactions">The new transactions.</param>
        private static void SaveContributions( IEnumerable<FinancialTransaction> newTransactions )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialTransactions.AddRange( newTransactions );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Maps the pledge.
        /// </summary>
        /// <param name="queryable">The queryable.</param>
        /// <param name="tableData"></param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void MapPledge( IQueryable<Row> tableData )
        {
            var lookupContext = new RockContext();
            var accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();

            var pledgeFrequencies = DefinedTypeCache.Read( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_FREQUENCY ), lookupContext ).DefinedValues;
            int oneTimePledgeFrequencyId = pledgeFrequencies.FirstOrDefault( f => f.Guid == new Guid( Rock.SystemGuid.DefinedValue.TRANSACTION_FREQUENCY_ONE_TIME ) ).Id;

            var newPledges = new List<FinancialPledge>();

            int completed = 0;
            int totalRows = tableData.Count();
            int percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying pledge import ({0:N0} found).", totalRows ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                decimal? amount = row["Total_Pledge"] as decimal?;
                DateTime? startDate = row["Start_Date"] as DateTime?;
                DateTime? endDate = row["End_Date"] as DateTime?;
                if ( amount != null && startDate != null && endDate != null )
                {
                    int? individualId = row["Individual_ID"] as int?;
                    int? householdId = row["Household_ID"] as int?;

                    var personKeys = GetPersonKeys( individualId, householdId, includeVisitors: false );
                    if ( personKeys != null && personKeys.PersonAliasId > 0 )
                    {
                        var pledge = new FinancialPledge();
                        pledge.PersonAliasId = personKeys.PersonAliasId;
                        pledge.CreatedByPersonAliasId = ImportPersonAliasId;
                        pledge.ModifiedDateTime = ImportDateTime;
                        pledge.StartDate = ( DateTime ) startDate;
                        pledge.EndDate = ( DateTime ) endDate;
                        pledge.TotalAmount = ( decimal ) amount;
                        pledge.CreatedDateTime = ImportDateTime;
                        pledge.ModifiedDateTime = ImportDateTime;
                        pledge.ModifiedByPersonAliasId = ImportPersonAliasId;

                        string frequency = row["Pledge_Frequency_Name"].ToString().ToLower();
                        if ( frequency != null )
                        {
                            frequency = frequency.ToLower();
                            if ( frequency.Equals( "one time" ) || frequency.Equals( "as can" ) )
                            {
                                pledge.PledgeFrequencyValueId = oneTimePledgeFrequencyId;
                            }
                            else
                            {
                                pledge.PledgeFrequencyValueId = pledgeFrequencies
                                    .Where( f => f.Value.ToLower().StartsWith( frequency ) || f.Description.ToLower().StartsWith( frequency ) )
                                    .Select( f => f.Id ).FirstOrDefault();
                            }
                        }

                        string fundName = row["Fund_Name"] as string;
                        string subFund = row["Sub_Fund_Name"] as string;
                        var fundIsActive = row["Fund_Is_active"] as bool?;
                        string fundType = row["FundType"] as string;

                        string fundGlAccount = row["Fund_GL_Account"] as string;
                        string subFundGLAccount = row["Sub_Fund_GL_Account"] as string;
                        if ( fundName != null )
                        {
                            var parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName ) );
                            if ( parentAccount == null )
                            {
                                parentAccount = AddAccount( lookupContext, fundName, fundGlAccount, null, null, fundIsActive, fundType );
                                accountList.Add( parentAccount );
                            }

                            if ( subFund != null )
                            {

                                var subFundIsActive = row["Sub_Fund_Is_active"] as bool?;
                                int? campusFundId = null;
                                // assign a campus if the subfund is a campus fund
                                var campusFund = CampusList.FirstOrDefault( c => subFund.StartsWith( c.Name ) || subFund.StartsWith( c.ShortCode ) );
                                if ( campusFund != null )
                                {
                                    // use full campus name as the subfund
                                    subFund = campusFund.Name;
                                    campusFundId = campusFund.Id;
                                }

                                // add info to easily find/assign this fund in the view
                                subFund = subFund.Truncate( 50 );

                                var childAccount = accountList.FirstOrDefault( c => c.Name.Equals( subFund ) && c.ParentAccountId == parentAccount.Id );
                                if ( childAccount == null )
                                {
                                    // create a child account with a campusId if it was set
                                    childAccount = AddAccount( lookupContext, subFund, subFundGLAccount, campusFundId, parentAccount.Id, subFundIsActive, fundType );
                                    accountList.Add( childAccount );
                                }

                                pledge.AccountId = childAccount.Id;
                            }
                            else
                            {
                                pledge.AccountId = parentAccount.Id;
                            }
                        }

                        newPledges.Add( pledge );
                        completed++;
                        if ( completed % percentage < 1 )
                        {
                            int percentComplete = completed / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} pledges imported ({1}% complete).", completed, percentComplete ) );
                        }
                        else if ( completed % ReportingNumber < 1 )
                        {
                            SavePledges( newPledges );
                            ReportPartialProgress();
                            newPledges.Clear();
                        }
                    }
                }
            }

            if ( newPledges.Any() )
            {
                SavePledges( newPledges );
            }

            ReportProgress( 100, string.Format( "Finished pledge import: {0:N0} pledges imported.", completed ) );
        }

        /// <summary>
        /// Saves the pledges.
        /// </summary>
        /// <param name="newPledges">The new pledges.</param>
        private static void SavePledges( List<FinancialPledge> newPledges )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPledges.AddRange( newPledges );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Adds the account.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="fundName">Name of the fund.</param>
        /// <param name="fundCampusId">The fund campus identifier.</param>
        /// <returns></returns>
        private static FinancialAccount AddAccount( RockContext lookupContext, string fundName, string accountGL, int? fundCampusId, int? parentAccountId, bool? isActive, string fundType )
        {
            lookupContext = lookupContext ?? new RockContext();

            var account = new FinancialAccount();
            account.Name = fundName;
            account.GlCode = accountGL;
            account.PublicName = fundName;
            account.IsTaxDeductible = string.IsNullOrEmpty( fundType ) || fundType != "Receipt";
            account.IsActive = isActive ?? true;
            account.CampusId = fundCampusId ?? GetFundId( fundName );
            account.ParentAccountId = parentAccountId;
            account.CreatedByPersonAliasId = ImportPersonAliasId;

            lookupContext.FinancialAccounts.Add( account );
            lookupContext.SaveChanges( DisableAuditing );

            return account;
        }

        private static int? GetFundId( string fundName )
        {
            if ( fundName == "4 - TCE - Contributions" || fundName == "Si Se Puede " )
            {
                return CampusList.AsQueryable().FirstOrDefault( c => c.ShortCode == "TCE" )?.Id;
            }
            return CampusList.AsQueryable().FirstOrDefault( c => c.ShortCode == "MAIN" )?.Id;
        }
    }
}