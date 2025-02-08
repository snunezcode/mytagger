import {memo,useState,useEffect, useRef} from 'react';

import CodeEditor from '@cloudscape-design/components/code-editor';
import 'ace-builds/css/ace.css';
import 'ace-builds/css/theme/cloud_editor.css';
import 'ace-builds/css/theme/cloud_editor_dark.css';


const i18nStrings = {
  loadingState: 'Loading code editor',
  errorState: 'There was an error loading the code editor.',
  errorStateRecovery: 'Retry',

  editorGroupAriaLabel: 'Code editor',
  statusBarGroupAriaLabel: 'Status bar',

  cursorPosition: (row, column) => `Ln ${row}, Col ${column}`,
  errorsTab: 'Errors',
  warningsTab: 'Warnings',
  preferencesButtonAriaLabel: 'Preferences',

  paneCloseButtonAriaLabel: 'Close',

  preferencesModalHeader: 'Preferences',
  preferencesModalCancel: 'Cancel',
  preferencesModalConfirm: 'Confirm',
  preferencesModalWrapLines: 'Wrap lines',
  preferencesModalTheme: 'Theme',
  preferencesModalLightThemes: 'Light themes',
  preferencesModalDarkThemes: 'Dark themes',
};
                                                                
const Component = memo(({ format="javascript", value ="", height = 500, readOnly=true,header = null, onChange = () => {} }) => {

    
    // Variables
    const [valueInt, setValueInt] = useState('');
    const [preferences, setPreferences] = useState({});
    const [loading, setLoading] = useState(true);
    const [ace, setAce] = useState();
    var currentAce = useRef({});


    function onDelayedChange(value){
      setValueInt(value);
      onChange(value);
    }
    
    function Initialization(){
        async function loadAce() {
          const ace = await import('ace-builds');
          await import('ace-builds/webpack-resolver');
          ace.config.set('useStrictCSP', true);
          ace.config.set('readOnly', readOnly);
          currentAce.current = ace;
          return ace;
        }

        loadAce()
          .then(ace => setAce(ace))
          .finally(() => setLoading(false));

    }

    // Initialization
    useEffect(() => {
            async function loadAce() {
              const ace = await import('ace-builds');
              await import('ace-builds/webpack-resolver');
              ace.config.set('useStrictCSP', true);
              ace.config.set('readOnly', readOnly);
              return ace;
            }
            loadAce()
              .then(ace => setAce(ace))
              .finally(() => setLoading(false));          
    }, []);


    useEffect(() => {
      if (ace && ace.edit) {
        const editor = ace.edit(document.querySelector('.ace_editor'));
        if (editor) {
          editor.setReadOnly(readOnly);
        }
      }
    }, [ace, readOnly]);
    
  

    useEffect(() => {
        setValueInt(value);
    }, [value]);
    
    return (
              <div>                  
                  {header}
                  <br/>
                  <CodeEditor
                      ace={ace}
                      value={valueInt}
                      language={format}
                      onDelayedChange={event => 
                                                { 
                                                  onDelayedChange(event.detail.value);

                                                }
                      }
                      preferences={preferences}
                      onPreferencesChange={event => setPreferences(event.detail)}
                      loading={loading}
                      i18nStrings={i18nStrings}
                      themes={{ light: ['cloud_editor'], dark: ['cloud_editor_dark'] }}
                      editorContentHeight={height}
                  />
              </div>

           );
});

export default Component;
