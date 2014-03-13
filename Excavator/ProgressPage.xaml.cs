﻿// <copyright>
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
using System.ComponentModel;
using System.Configuration;
using System.Windows;
using System.Windows.Controls;

namespace Excavator
{
    /// <summary>
    /// Interaction logic for TransformPage.xaml
    /// </summary>
    public partial class ProgressPage : System.Windows.Controls.Page
    {
        public ExcavatorComponent excavator;

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="TransformPage"/> class.
        /// </summary>
        public ProgressPage( ExcavatorComponent parameter = null )
        {
            InitializeComponent();
            if ( parameter != null )
            {
                var importUser = ConfigurationManager.AppSettings["ImportUser"];

                excavator = parameter;
                excavator.TransformData( importUser );
            }
        }

        #endregion

        #region Events

        /// <summary>
        /// Handles the Click event of the btnStart control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnNext_Click( object sender, RoutedEventArgs e )
        {
            //var btnSender = (Button)sender;
            //if ( btnSender != null && (string)btnSender.Content == "Start" )
            //{
            //    btnStart.Content = "Cancel";
            //    btnStart.Style = (Style)FindResource( "buttonStyle" );

            //    BackgroundWorker bwTransformData = new BackgroundWorker();
            //    bwTransformData.DoWork += bwTransformData_DoWork;
            //    bwTransformData.ProgressChanged += bwTransformData_ProgressChanged;
            //    bwTransformData.RunWorkerCompleted += bwTransformData_RunWorkerCompleted;
            //    bwTransformData.RunWorkerAsync();
            //}
            //else
            //{
            //    btnClose_Click( sender, e );
            //}
        }

        /// <summary>
        /// Handles the Click event of the btnBack control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnBack_Click( object sender, RoutedEventArgs e )
        {
            this.NavigationService.GoBack();
        }

        /// <summary>
        /// Handles the Click event of the btnNext control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RoutedEventArgs"/> instance containing the event data.</param>
        private void btnClose_Click( object sender, RoutedEventArgs e )
        {
            // Should this run in background until finished?
            // if not then at least wait until all currently processing models have been saved?
            Application.Current.Shutdown();
        }

        #endregion

        #region Async Tasks

        /// <summary>
        /// Handles the DoWork event of the bwTransformData control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="DoWorkEventArgs"/> instance containing the event data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private void bwTransformData_DoWork( object sender, DoWorkEventArgs e )
        {
            var importUser = ConfigurationManager.AppSettings["ImportUser"];
            bool isComplete = excavator.TransformData( importUser );
        }

        /// <summary>
        /// Handles the ProgressChanged event of the bwTransformData control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="ProgressChangedEventArgs"/> instance containing the event data.</param>
        private void bwTransformData_ProgressChanged( object sender, ProgressChangedEventArgs e )
        {
            //lblUploadProgress.Content = string.Format( "Uploading Scanned Checks {0}%", e.ProgressPercentage );
        }

        /// <summary>
        /// Handles the RunWorkerCompleted event of the bwTransformData control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="RunWorkerCompletedEventArgs"/> instance containing the event data.</param>
        private void bwTransformData_RunWorkerCompleted( object sender, RunWorkerCompletedEventArgs e )
        {
            this.Dispatcher.Invoke( (Action)( () =>
            {
                lblProgress.Visibility = Visibility.Visible;
                btnClose.Visibility = Visibility.Visible;
            } ) );

            BackgroundWorker bwTransformData = sender as BackgroundWorker;
            bwTransformData.RunWorkerCompleted -= new RunWorkerCompletedEventHandler( bwTransformData_RunWorkerCompleted );
            bwTransformData.ProgressChanged -= new ProgressChangedEventHandler( bwTransformData_ProgressChanged );
            bwTransformData.DoWork -= new DoWorkEventHandler( bwTransformData_DoWork );
            bwTransformData.Dispose();
        }

        #endregion
    }
}